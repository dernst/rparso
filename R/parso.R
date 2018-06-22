#' @import rJava
onLoad = function(libname, pkgname) {

    mycp = paste0(c(system.file("java", "parso-2.0.9.jar", package="rparso"),
                    system.file("java", "slf4j-api-1.7.25.jar", package="rparso"),
                    system.file("java", "rparso.jar", package="rparso"),
                    "."),
                  collapse=":")
    Sys.setenv("CLASSPATH"=mycp)

    options(java.parameters = "-Xmx2048m")
    rJava::.jinit()
    #.jaddClassPath(system.file("java", "parso-2.0.9.jar", package="rparso"))
    #.jaddClassPath(system.file("java", "slf4j-api-1.7.25.jar", package="rparso"))
    #.jaddClassPath(system.file("java", "rparso.jar", package="rparso"))
}

j_read_chunk = function(obj)
    .jcall(tst, "I", "read_chunk")

j_get_ints = function(obj, col)
    .jcall(tst, "[I", "getInts", as.integer(col-1L))

j_get_strings = function(obj, col)
    .jcall(tst, "[Ljava/lang/String;", "getStrings", as.integer(col-1L))

j_get_string = function(obj, col)
    .jcall(tst, "Ljava/lang/String;", "getString", as.integer(col-1L))

read_parso = function(filename) {
    options(java.parameters = "-Xmx2048m")
    library(rparso)
    library(rJava)
    library(data.table)
    tst = .jnew("de/misc/rparso/BulkRead", "/home/ernst/mnt/bvs/Daten/DBABZUG20180619/tsch.sas7bdat")
    cn = .jcall(tst, "[Ljava/lang/String;", method="getColnames")
    ct = .jcall(tst, "[Ljava/lang/String;", method="getColtypes")
    num_rows = .jcall(tst, "J", method="getNumRows")

    dataframes = list()
    rows_total = 0L

    #Rprof("test.prof")
    print(system.time({
    while(TRUE) {
        rows_read = .jcall(tst, "I", "read_chunk")
        if(rows_read <= 0)
            break

        rows_total = rows_read + rows_total
        if(rows_total %% (4096*10) == 0)
            cat(sprintf("%d\n", rows_total))

        lst = lapply(seq_along(ct), function(i) {
            cti = ct[i]
            if(cti == "java.lang.Number")
                j_get_ints(tst, i)
            else
                j_get_string(tst, i)

                #strsplit(j_get_string(tst, i), "\n", fixed=TRUE)[[1]]
        })
        names(lst) = cn
        #dataframes[[length(dataframes)+1L]] = as.data.table(lst)
        dataframes[[length(dataframes)+1L]] = lst
        #xx = .jcall(tst, "[Ljava/lang/String;", "getStrings", 2L)

        #xx = .jcall(tst, "[I", "getInts", 0L)
    }
    }))
    #Rprof(NULL)

}


if(FALSE) {
    library(rJava)

    .jnew("de/misc/rparso/BulkRead", "s")

    .Call("parso_read_sas", "/home/ernst/mnt/bvs/Daten/DBABZUG20180619/tsch.sas7bdat")

    .jmethods("Ljava/io/FileInputStream;")

}
