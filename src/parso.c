#include <Rinternals.h>
#include <jni.h>
#include <stdlib.h>

SEXP current_df = NULL;
char *string_buffer = NULL;
int string_buffer_size = 0;

void cb_set_numeric(JNIEnv *env, jobject obj, jint col, jint row, jdouble num); 
void cb_set_int(JNIEnv *env, jobject obj, jint col, jint row, jint num); 
void cb_set_string(JNIEnv *env, jobject obj, jint col, jint row, jstring str);
void cb_set_bytes(JNIEnv *env, jobject obj, jint col, jint row, jbyteArray str);


static int register_natives(JNIEnv *env) {
    jclass cls = (*env)->FindClass(env, "de/misc/rparso/BulkRead");

    JNINativeMethod my_natives[4];
    my_natives[0].name = "cb_set_int";
    my_natives[0].signature = "(III)V";
    my_natives[0].fnPtr = (void*)cb_set_int;

    my_natives[1].name = "cb_set_string";
    my_natives[1].signature = "(IILjava/lang/String;)V";
    my_natives[1].fnPtr = cb_set_string;

    my_natives[2].name = "cb_set_bytes";
    my_natives[2].signature = "(II[B)V";
    my_natives[2].fnPtr = cb_set_bytes;

    my_natives[3].name = "cb_set_numeric";
    my_natives[3].signature = "(IID)V";
    my_natives[3].fnPtr = (void*)cb_set_numeric;


    if((*env)->RegisterNatives(env, cls, my_natives, 4) != 0) {
        Rprintf("RegisterNatives failed\n");
        return 0;
    }
    return 1;
}

jint JNI_OnLoad(JavaVM* jvm, void* foo) {
    //puts("JNI_OnLoad was here");
    JNIEnv *env = NULL;
    (*jvm)->AttachCurrentThread(jvm, (void*)&env, 0);
    if(env == NULL) {
        Rprintf("cant init rparso/JNI (error in AttachCurrentThread)\n");
        return 0;
    }

    if(!register_natives(env)) {
        return 0;
    }

    return JNI_VERSION_1_8;
}

/*
JNIEnv* getEnv() {
    JavaVM *jvm = NULL;
    JNIEnv *env = NULL;
    jsize l;
    jint res=-1;

    res = JNI_GetCreatedJavaVMs(&jvm, 1, &l);
    if(res != 0) {
        Rprintf("JNI_GetCreatedJavaVMs failed\n");
        return NULL;
    }
    if(l < 1) {
        Rprintf("no java VM yet\n");
        return NULL;
    }

    res = (*jvm)->AttachCurrentThread(jvm, (void*)&env, 0);
    if(res != 0) {
        Rprintf("AttachCurrentThread failed\n");
        return NULL;
    }


    return env;
}
*/

SEXP rparso_set_df(SEXP df) {
    current_df = df;
    return R_NilValue;
}

SEXP rparso_cleanup() {
    current_df = R_NilValue;
    if(string_buffer != NULL) {
        free(string_buffer);
    }
    string_buffer = NULL;
    string_buffer_size = -1;
    return R_NilValue;
}

/*
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
*/

/*
int parso_num_rows(JNIEnv *env, jclass cls, jobject obj) {
    jmethodID methodID = (*env)->GetMethodID(env, cls, "getNumRows", "()J");
    if(methodID == NULL) {
        return -1;
    }

    jlong l = (*env)->CallLongMethod(env, obj, methodID);
    return (int)l;
}
*/

void cb_set_int(JNIEnv *env, jobject obj, jint col, jint row, jint num) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    INTEGER(r_col)[row] = num;
    return;
}

void cb_set_numeric(JNIEnv *env, jobject obj, jint col, jint row, jdouble num) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    REAL(r_col)[row] = num;
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

static void string_buffer_ensure_size(int s) {
    const int block_size = 4096;
    //int new_size = s - (s % block_size) * block_size;

    if(string_buffer == NULL) {
        string_buffer_size = (s / block_size + 1) * block_size;
        if(0) Rprintf("allocating s=%d, new size=%d\n", s, string_buffer_size);
        string_buffer = (char*) malloc(sizeof(char) * string_buffer_size);
    } else if(string_buffer_size < s){
        if(0) Rprintf("growing buffer for s=%d (current size=%d)\n", s, string_buffer_size);
        string_buffer_size = (s / block_size + 1) * block_size;
        string_buffer = realloc(string_buffer, sizeof(char)*string_buffer_size);
    }
}

void cb_set_bytes(JNIEnv *env, jobject obj, jint col, jint row, jbyteArray str) {
    if(str == NULL) {
        return;
    }

    int len = (*env)->GetArrayLength(env, str);
    string_buffer_ensure_size(len+1);

    (*env)->GetByteArrayRegion(env, str, 0, len, (jbyte*)string_buffer);

    SEXP r_col = VECTOR_ELT(current_df, col);
    SEXP foo = mkCharLenCE(string_buffer, len, CE_UTF8);
    SET_STRING_ELT(r_col, row, foo);
}


/*
SEXP parso_init(SEXP cp) {
    startJVM(CHAR(STRING_ELT(cp, 0)));
    return R_NilValue;
}
*/

#if 0
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
#endif /* 0 */


static const R_CallMethodDef callMethods[]  = {
      {"rparso_set_df", (DL_FUNC) &rparso_set_df, 1},
      {"rparso_cleanup", (DL_FUNC) &rparso_cleanup, 0},
      {NULL, NULL, 0}
};

void
R_init_rparso(DllInfo *info)
{
    R_registerRoutines(info, NULL, callMethods, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
    R_forceSymbols(info, TRUE);
}

