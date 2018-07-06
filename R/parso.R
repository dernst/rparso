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
read_parso = function(filename, nrow=NULL, skip=NULL, select=NULL, encoding=NULL) {
    if(!is.null(encoding)) {
        tst = .jnew("de/misc/rparso/BulkRead", filename, encoding)
    } else {
        tst = .jnew("de/misc/rparso/BulkRead", filename)
    }

    if(!is.null(nrow))
        .jcall(tst, "V", "setMaxRows", as.integer(nrow))

    if(!is.null(skip))
        .jcall(tst, "V", "setSkipRows", as.integer(skip))

    num_rows_total = .jcall(tst, "J", "getNumRows")
    num_rows = .jcall(tst, "I", "getActualRows")
    #cat(sprintf("reading %d of %d rows\n", num_rows, num_rows_total))
    colnames_ = .jcall(tst, "[S", "getColnames")

    if(!is.null(select)) {
        extraneous_columns = setdiff(select, colnames_)
        if(length(extraneous_columns) > 0) {
            warning(sprintf("extra columns specified that do not exist: %s",
                            paste0(extraneous_columns, collapse=", ")))
        }

        colnames_ = intersect(select, colnames_)
        .jcall(tst, "V", "setSelectColnames", colnames_)
    }

    xdf = lapply(.jcall(tst, "[Ljava/lang/String;", "getColtypes"), function(ct) {
        if(ct == "java.lang.Number") {
            double(num_rows)
        } else {
            character(num_rows)
        }
    })

    .Call("rparso_set_df", xdf)
    on.exit(.Call("rparso_cleanup"))

    #print(system.time({
        xx = .jcall(tst, "I", "read_all")
    #}))


    names(xdf) = colnames_
    setDT(xdf)
    xdf
}


if(FALSE) {
    x = read_parso(path.expand("~/mnt/bvs/Daten/DBABZUG20180627/tsch.sas7bdat"))
    x = read_parso(path.expand("~/mnt/bvs/Daten/DBABZUG20180627/tschind.sas7bdat"))

}

