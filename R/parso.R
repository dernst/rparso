#' @import rJava
#' @import data.table
#' @useDynLib rparso
NULL


.onLoad = function(libname, pkgname) {
    .jinit()
    .jaddClassPath(system.file("java", "parso-2.0.9.jar", package="rparso"))
    .jaddClassPath(system.file("java", "slf4j-api-1.7.25.jar", package="rparso"))
    .jaddClassPath(system.file("java", "slf4j-nop-1.7.25.jar", package="rparso"))
    .jaddClassPath(system.file("java", "rparso.jar", package="rparso"))

    fn = system.file("libs", "rparso.so", package="rparso")
    if(!file.exists(fn)) {
        fn = system.file("src", "rparso.so", package="rparso")
        if(!file.exists(fn))
            stop("cant find rparso.so")
    }

    .jcall("de/misc/rparso/BulkRead", "V", "init", fn)
}

#' Read a sa7bdat file via EPAMs parso library
#' @param filename The filename of the file to read
#' @export
read_sas = function(filename) {
    tst = .jnew("de/misc/rparso/BulkRead", filename)
    num_rows = .jcall(tst, "J", "getNumRows")
    colnames_ = .jcall(tst, "[S", "getColnames")
    xdf = lapply(.jcall(tst, "[Ljava/lang/String;", "getColtypes"), function(ct) {
        if(ct == "java.lang.Number") {
            integer(num_rows)
        } else {
            character(num_rows)
        }
    })

    .Call("rparso_set_df", xdf)

    print(system.time({
        xx = .jcall(tst, "I", "read_all")
    }))

    names(xdf) = colnames_
    setDT(xdf)
    xdf
}


if(FALSE) {
    x = read_sas(path.expand("~/mnt/bvs/Daten/DBABZUG20180627/tsch.sas7bdat"))
    x = read_sas(path.expand("~/mnt/bvs/Daten/DBABZUG20180627/tschind.sas7bdat"))

}

