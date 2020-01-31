
val Datos_ficticios = sc.textFile("hdfs:///eoi/Proyecto-Final/Datos_ficticios.csv")

Datos_ficticios.count

val Datos_agua = sc.textFile("hdfs:///eoi/Proyecto-Final/2013agua.csv")

Datos_agua.count

val Datos_electricidad = sc.textFile("hdfs:///eoi/Proyecto-Final/2013electricidad.csv")

Datos_electricidad.count

val Datos_ficticios_cabecera = Datos_ficticios.first() 

val Datos_ficticios_ok = Datos_ficticios.filter( _ != Datos_ficticios_cabecera)

Datos_ficticios_ok.count

val d_ficticios = Datos_ficticios_ok.map(x => x.split(";")).cache

d_ficticios.take(5) 

val id_edificio = d_ficticios.keyBy( _ (0))

id_edificio.take(5)

val dat_agua = Datos_agua.map(x => x.split(";")).cache

dat_agua.take(5)

val d_agua = dat_agua.filter(_(0)!="AGUA_UPO")

d_agua.count()

val d_agua_UPO = dat_agua.filter(_(0)=="AGUA_UPO")

d_agua_UPO.count()

val dat_electricidad = Datos_electricidad.map(x => x.split(";")).cache

dat_electricidad.take(5)

val d_electricidad = dat_electricidad.filter(x => (x(0)!="ELEC_UPO"))

d_electricidad.count()

val d_electricidad_UPO = dat_electricidad.filter(x => (x(0)=="ELEC_UPO"))

d_electricidad_UPO.count()

val agua_por_edificio_1 = d_agua.map(x => (x(0),x(1).toDouble))

agua_por_edificio.take(5)

val count_neg = agua_por_edificio.filter(x => x._2 <0).count()

val agua_por_edificio = agua_por_edificio_1.filter(x => x._2 >= 0)

agua_por_edificio.count()

val total_agua_por_edificio = agua_por_edificio.reduceByKey((a,b) => a+b).sortBy(x => x._2,false)

total_agua_por_edificio.take(28)

val media_agua_edificios = total_agua_por_edificio.values.mean

val max_agua_edificios = total_agua_por_edificio.reduceByKey((a,b) => a+b).sortBy(x => x._2,false).first()

val min_agua_edificios = total_agua_por_edificio.reduceByKey((a,b) => a+b).sortBy(x => x._2,true).first()

val des_agua_edificios = total_agua_por_edificio.values.stdev

val total_agua = agua_por_edificio.values.reduce( (a,b) => a + b)

val neg_upo_agua = d_agua_UPO.filter(x => x(1).toDouble < 0 ).count()

d_agua_UPO.take(5)

val total_agua_upo = d_agua_UPO.map(x => (x(0),x(1).toDouble)).values.reduce((a,b) => a + b)

val total_agua_no_supervisada = total_agua_upo - total_agua

val elec_por_edificio_1 = d_electricidad.map(x => (x(0),x(1).toDouble))

val neg_elec = elec_por_edificio_1.filter(x => x._2 <0).count()

val elec_por_edificio = elec_por_edificio_1.filter(x => x._2 >= 0)

elec_por_edificio.count()

val total_elec_por_edificio = elec_por_edificio.reduceByKey( (a,b) => a+b).sortBy(x => x._2,false)

total_elec_por_edificio.take(30)

val media_elec_edificios = total_elec_por_edificio.values.mean

val max_elec_edificios = total_elec_por_edificio.reduceByKey((a,b) => a+b).sortBy(x => x._2,false).first()

val min_elec_edificios = total_elec_por_edificio.reduceByKey((a,b) => a+b).sortBy(x => x._2,true).first()

val des_elec_edificios = total_elec_por_edificio.values.stdev

val total_elec = elec_por_edificio.values.reduce( (a,b) => a + b)

val neg_upo_elec = d_electricidad_UPO.filter(x => x(1).toDouble < 0 ).count()

val total_elec_upo = d_electricidad_UPO.map(x => (x(0),x(1).toDouble)).values.reduce((a,b) => a + b)

val total_elec_no_supervisada = total_elec_upo - total_elec

val agua_sin_ = Datos_agua.map(x => x.replace( "AGUA_", "" ))

val agua_edif_sin = agua_sin_.map(x => x.split(";")).cache

agua_edif_sin.take(10)

val agua_edif_sin_N_UPO = agua_edif_sin.filter(x=>(x(0)!="UPO"))

val agua_por_edifi_SIN = agua_edif_sin_N_UPO.map(x => (x(0),x(1).toDouble))

val total_agua_por_edif_SIN = agua_por_edifi_SIN.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2,false)

total_agua_por_edif_SIN.take(28)

val agua_ficticios = d_ficticios.map(x => (x(0), x(1)))

agua_ficticios.take(28)

val union_agua_ficticios =  total_agua_por_edif_SIN.join(agua_ficticios)

union_agua_ficticios.take(5)

val agua_direccion = union_agua_ficticios.map(x=> (( x._2 ).toString.split( "," )))

agua_direccion.take(28)

val agua_direc = agua_direccion.map(x => ( x(1).replace( ")" , "" ) , x(0).replace( "(" , "" ).toDouble))

agua_direc.take(5)

agua_direc.sortBy(_._2,false)

agua_direc.take(28)

val sumatorio = agua_direc.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2,false)

sumatorio.take(28)

val elec_sin_ = Datos_electricidad.map(x => x.replace( "ELEC_", "" ))

val elec_edif_sin = elec_sin_.map(x => x.split(";")).cache

elec_edif_sin.take(10)

val elec_edif_sin_N_UPO = elec_edif_sin.filter(x=>(x(0)!="UPO"))

val elec_por_edifi_SIN = elec_edif_sin_N_UPO.map(x => (x(0),x(1).toDouble))

val total_elec_por_edif_SIN = elec_por_edifi_SIN.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2,false)

total_elec_por_edif_SIN.take(28)

val elec_ficticios = d_ficticios.map(x => (x(0), x(1)))

elec_ficticios.take(3)

val union_elec_ficticios =  total_elec_por_edif_SIN.join(agua_ficticios)

union_elec_ficticios.take(5)

val elec_direccion = union_elec_ficticios.map(x=> (( x._2 ).toString.split( "," )))

elec_direccion.take(5)

val elec_direc = elec_direccion.map(x => ( x(1).replace( ")" , "" ) , x(0).replace( "(" , "" ).toDouble))

elec_direc.sortBy(_._2,false)

elec_direc.take(28)

val sumatorio2 = elec_direc.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2,false)

sumatorio2.take(28)
