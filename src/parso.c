#include <Rinternals.h>
#include <jni.h>

JNIEnv* getEnv() {
    JavaVM *jvm = NULL;
    JNIEnv *env = NULL;
    jsize l;
    jint res=-1;

    res = JNI_GetCreatedJavaVMs(&jvm, 1, &l);
    if(res != 0) {
        puts("JNI_GetCreatedJavaVMs failed");
        return NULL;
    }
    if(l < 1) {
        puts("no java VM yet");
        return NULL;
    }

    res = (*jvm)->AttachCurrentThread(jvm, (void*)&env, 0);
    if(res != 0) {
        puts("AttachCurrentThread failed");
        return NULL;
    }


    return env;
}

SEXP parso_read_sas(SEXP filename) {
    if(!Rf_isString(filename)) {
        return R_NilValue;
    }

    printf("%s\n", CHAR(STRING_ELT(filename, 0)));

    JNIEnv *env = getEnv();
    if(env == NULL) {
        return R_NilValue;
    }

    jclass cls = (*env)->FindClass(env, "de/misc/rparso/BulkRead");
    //jclass cls = (*env)->FindClass(env, "com/epam/parso/SasFileReader");
    if(cls == NULL) {
        puts("cls NULL");
        if((*env)->ExceptionOccurred(env))
            (*env)->ExceptionDescribe(env);
        return R_NilValue;
    }

    jmethodID methodID = (*env)->GetMethodID(env, cls, "<init>", "(Ljava/lang/String;)V");
    if(methodID == NULL) {
        puts("methodID NULL");
        return R_NilValue;
    }

    jstring jfilename = (*env)->NewStringUTF(env, CHAR(STRING_ELT(filename, 0)));
    if(jfilename == NULL) {
        puts("jfilename NULL");
        return R_NilValue;
    }
    jobject a = (*env)->NewObject(env, cls, methodID, jfilename);
    if(a == NULL) {
        puts("a is NULL");
    }

    return R_NilValue;
}

