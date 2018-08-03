
if(FALSE) {

    fn = system.file("examples", "iris.sas7bdat", package="haven")
    x = read_parso(fn)

    x = read_parso(fn, nrow=10)

    x = read_parso(fn, nrow=10, skip=10)

    x = read_parso(fn, nrow=160, skip=10)

    x = read_parso(fn, select=c("Sepal_Length", "Petal_Length"))
    x = read_parso(fn, select=c("Petal_Length", "Sepal_Length"))


    gcinfo(TRUE)
    x = read_parso(path.expand("~/mnt/bvs/Daten/DBABZUG20180627/tsch.sas7bdat"), encoding="UTF-8")
    gc.time(TRUE)


    print(system.time({
        x = read_parso(path.expand("~/mnt/bvs/Daten/DBABZUG20180627/tsch.sas7bdat"), encoding="UTF-8")
    }))


}

