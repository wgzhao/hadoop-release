/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with this
* work for additional information regarding copyright ownership. The ASF
* licenses this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

#include <jni.h>
#include "org_apache_hadoop.h"
#include "winutils_process_stub.h"
#include "winutils.h"
#include "file_descriptor.h"

// class of org.apache.hadoop.io.nativeio.NativeIO.WinutilsProcessStub
static jclass wps_class = NULL;


static jmethodID wps_constructor = NULL;
static jfieldID wps_hProcess = NULL;
static jfieldID wps_hThread = NULL;
static jfieldID wps_disposed = NULL;

extern void throw_ioe(JNIEnv* env, int errnum);

void winutils_process_stub_init(JNIEnv *env) {
  if (wps_class != NULL) return; // already initted

  wps_class = (*env)->FindClass(env, WINUTILS_PROCESS_STUB_CLASS);
  PASS_EXCEPTIONS(env);

  wps_class = (*env)->NewGlobalRef(env, wps_class);
  PASS_EXCEPTIONS(env);

  wps_hProcess = (*env)->GetFieldID(env, wps_class, "hProcess", "J");
  PASS_EXCEPTIONS(env);  

  wps_hThread = (*env)->GetFieldID(env, wps_class, "hThread", "J");
  PASS_EXCEPTIONS(env);  

  wps_disposed = (*env)->GetFieldID(env, wps_class, "disposed", "Z");
  PASS_EXCEPTIONS(env); 

  wps_constructor = (*env)->GetMethodID(env, wps_class, "<init>", "(JJJJJ)V");
  PASS_EXCEPTIONS(env);

  LogDebugMessage(L"winutils_process_stub_init\n");
}

void winutils_process_stub_deinit(JNIEnv *env) {
  if (wps_class != NULL) {
    (*env)->DeleteGlobalRef(env, wps_class);
    wps_class = NULL;
  }
  wps_hProcess = NULL;
  wps_hThread = NULL;
  wps_disposed = NULL;
  wps_constructor = NULL;
  LogDebugMessage(L"winutils_process_stub_deinit\n");
}

jobject winutils_process_stub_create(JNIEnv *env, 
  jlong hProcess, jlong hThread, jlong hStdIn, jlong hStdOut, jlong hStdErr) {
  jobject obj = (*env)->NewObject(env, wps_class, wps_constructor, 
    hProcess, hThread, hStdIn, hStdOut, hStdErr);
  PASS_EXCEPTIONS_RET(env, NULL);

  LogDebugMessage(L"winutils_process_stub_create: %p\n", obj);

  return obj;
}


/*
 * native void destroy();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024WinutilsProcessStub_destroy(
  JNIEnv *env, jobject objSelf) {

  HANDLE hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
  LogDebugMessage(L"TerminateProcess: %x\n", hProcess);
  TerminateProcess(hProcess, EXIT_FAILURE);
}

/*
 * native void waitFor();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024WinutilsProcessStub_waitFor(
  JNIEnv *env, jobject objSelf) {

  HANDLE hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
  LogDebugMessage(L"WaitForSingleObject: %x\n", hProcess);
  WaitForSingleObject(hProcess, INFINITE);
}



/*
 * native void resume();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024WinutilsProcessStub_resume(
  JNIEnv *env, jobject objSelf) {

  DWORD dwError;
  HANDLE hThread = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hThread);
  if (-1 == ResumeThread(hThread)) {
    dwError = GetLastError();
    LogDebugMessage(L"ResumeThread: %x error:%d\n", hThread, dwError);
    throw_ioe(env, dwError);
  }
}

/*
 * native int exitValue();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jint JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024WinutilsProcessStub_exitValue(
  JNIEnv *env, jobject objSelf) {

  DWORD exitCode;
  DWORD dwError;
  HANDLE hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
  if (!GetExitCodeProcess(hProcess, &exitCode)) {
    dwError = GetLastError();
    throw_ioe(env, dwError);
    return dwError; // exception was thrown, return value doesn't really matter
  }
  LogDebugMessage(L"GetExitCodeProcess: %x :%d\n", hProcess, exitCode);
  
  return exitCode;
}


/*
 * native void dispose();
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT void JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024WinutilsProcessStub_dispose(
  JNIEnv *env, jobject objSelf) {

  HANDLE hProcess = INVALID_HANDLE_VALUE, 
         hThread  = INVALID_HANDLE_VALUE;

  jboolean disposed = (*env)->GetBooleanField(env, objSelf, wps_disposed);

  if (JNI_TRUE != disposed) {
    hProcess = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hProcess);
    hThread = (HANDLE)(*env)->GetLongField(env, objSelf, wps_hThread);

    CloseHandle(hProcess);
    CloseHandle(hThread);
    (*env)->SetBooleanField(env, objSelf, wps_disposed, JNI_TRUE);
    LogDebugMessage(L"disposed: %p\n", objSelf);
  }
}


/*
 * native static FileDescriptor getFileDescriptorFromHandle(long handle);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jobject JNICALL 
Java_org_apache_hadoop_io_nativeio_NativeIO_00024WinutilsProcessStub_getFileDescriptorFromHandle(
  JNIEnv *env, jclass klass, jlong handle) {

  LogDebugMessage(L"getFileDescriptorFromHandle: %x\n", handle);
  return fd_create(env, (long) handle);
}

