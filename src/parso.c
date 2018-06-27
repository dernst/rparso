#include <Rinternals.h>
#include <jni.h>
#include <stdlib.h>


SEXP current_df = NULL;

void cb_set_int(JNIEnv *env, jobject obj, jint col, jint row, jint num); 
void cb_set_string(JNIEnv *env, jobject obj, jint col, jint row, jstring str);
void cb_set_bytes(JNIEnv *env, jobject obj, jint col, jint row, jbyteArray str);


int register_natives(JNIEnv *env) {
    jclass cls = (*env)->FindClass(env, "de/misc/rparso/BulkRead");
    JNINativeMethod my_natives[3];
    my_natives[0].name = "cb_set_int";
    my_natives[0].signature = "(III)V";
    my_natives[0].fnPtr = cb_set_int;

    my_natives[1].name = "cb_set_string";
    my_natives[1].signature = "(IILjava/lang/String;)V";
    my_natives[1].fnPtr = cb_set_string;

    my_natives[2].name = "cb_set_bytes";
    my_natives[2].signature = "(II[B)V";
    my_natives[2].fnPtr = cb_set_bytes;


    if((*env)->RegisterNatives(env, cls, my_natives, 3) != 0) {
        puts("RegisterNatives failed :(");
        return 0;
    }
    return 1;
}

jint JNI_OnLoad(JavaVM* jvm, void* foo) {
    puts("JNI_OnLoad was here");
    JNIEnv *env = NULL;
    (*jvm)->AttachCurrentThread(jvm, (void*)&env, 0);
    if(env == NULL) {
        puts("cant init rparso/JNI (error in AttachCurrentThread)");
        return 0;
    }
    register_natives(env);
    return JNI_VERSION_1_8;
}

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

SEXP rparso_set_df(SEXP df) {
    current_df = df;
    return R_NilValue;
}

void startJVM(const char *cp) {
    JavaVM *jvm;
    JNIEnv *env;
    char *cpopt = malloc(sizeof(char)*4096);
    sprintf(cpopt, "-Djava.class.path=/usr/lib/java:%s", cp);

    JavaVMInitArgs vm_args;
    JavaVMOption options[2];
    options[0].optionString = cpopt;
    options[1].optionString = "-Xmx2g";

    vm_args.version = JNI_VERSION_1_6;
    vm_args.nOptions = 2;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = FALSE;

    jint ret = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
    free(cpopt);

    printf("ret = %d (jvm=%p)\n", ret, (void*)jvm);

}

int parso_num_rows(JNIEnv *env, jclass cls, jobject obj) {
    jmethodID methodID = (*env)->GetMethodID(env, cls, "getNumRows", "()J");
    if(methodID == NULL) {
        return -1;
    }

    jlong l = (*env)->CallLongMethod(env, obj, methodID);
    return (int)l;
}

void cb_set_int(JNIEnv *env, jobject obj, jint col, jint row, jint num) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    INTEGER(r_col)[row] = num;
    return;
}

void cb_set_string(JNIEnv *env, jobject obj, jint col, jint row, jstring str) {
    if(str == NULL) {
        //puts("> str NULL");
        return;
    }
    const char *c = (*env)->GetStringUTFChars(env, str, NULL);
    if(c == NULL) {
        //puts("> c NULL");
        return;
    }
    //printf("%s\n", c);
    (*env)->ReleaseStringUTFChars(env, str, c);
    return;
}

void cb_set_bytes(JNIEnv *env, jobject obj, jint col, jint row, jbyteArray str) {
    static char buf[4096];
    if(str == NULL) {
        return;
    }

    int len = (*env)->GetArrayLength(env, str);
    if(len >= 4095)
        len=4095;
    //char *buf = (char*) malloc(sizeof(char)*len);
    (*env)->GetByteArrayRegion(env, str, 0, len, (jbyte*)buf);

    SEXP r_col = VECTOR_ELT(current_df, col);
    SET_STRING_ELT(r_col, row, mkCharLen(buf, len));
    //free(buf);
}


SEXP parso_init(SEXP cp) {
    startJVM(CHAR(STRING_ELT(cp, 0)));
    return R_NilValue;
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
    //jclass cls = (*env)->FindClass(env, "RJavaClassLoader");
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
        return R_NilValue;
    }

    JNINativeMethod my_natives[3];
    my_natives[0].name = "cb_set_int";
    my_natives[0].signature = "(JI)V";
    my_natives[0].fnPtr = cb_set_int;

    my_natives[1].name = "cb_set_string";
    my_natives[1].signature = "(JLjava/lang/String;)V";
    my_natives[1].fnPtr = cb_set_string;

    my_natives[2].name = "cb_set_bytes";
    my_natives[2].signature = "(J[B)V";
    my_natives[2].fnPtr = cb_set_bytes;


    if((*env)->RegisterNatives(env, cls, my_natives, 3) != 0) {
        puts("RegisterNatives failed :(");
        return R_NilValue;
    }

    methodID = (*env)->GetMethodID(env, cls, "read_all", "()I");
    if(methodID == NULL) {
        puts("cant find read_all method");
        return R_NilValue;
    }

    jint rows_read = (*env)->CallIntMethod(env, a, methodID);
    if((*env)->ExceptionOccurred(env)) {
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
            return R_NilValue;
    }

    printf("%d rows read\n", rows_read);

    /*
    methodID = (*env)->GetMethodID(env, cls, "getReader",
            "()Lcom/epam/parso/SasFileReader;");
    if(methodID == NULL) {
        puts("cant find getReader method");
        return R_NilValue;
    }

    jobject rdr = (*env)->CallObjectMethod(env, a, methodID);
    if(rdr == NULL) {
        puts("getReader() == NULL");
        return R_NilValue;
    } 

    jclass cls_sfr = (*env)->FindClass(env, "com/epam/parso/SasFileReader");
    if(cls_sfr == NULL) {
        puts("cant find sfr class");
        return R_NilValue;
    }

    methodID = (*env)->GetMethodID(env, cls_sfr, "readNext",
            "()[Ljava/lang/Object;");
    if(methodID == NULL) {
        puts("cant find readNext method");
        return R_NilValue;
    } 

    jobject obj;
    int cnt=0;

    while(1) {
        if(++cnt % 10000) {
            printf("%d\n", cnt);
        }
        obj = (*env)->CallObjectMethod(env, rdr, methodID);
        if((*env)->ExceptionOccurred(env)) {
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
            return R_NilValue;
        }
    }
    */

    puts("ok");
    return R_NilValue;
}

