import re
import tempfile
from pathlib import Path
from custom_command import LinkCountingCommand
from openwpm.command_sequence import CommandSequence, DumpPageSourceCommand
from openwpm.commands.browser_commands import GetCommand
from openwpm.config import BrowserParams, ManagerParams
from openwpm.storage.sql_provider import SQLiteStorageProvider
from openwpm.task_manager import TaskManager
from bs4 import BeautifulSoup
import os
from os import remove
import pandas as pd
import glob
import sqlite3
import shutil
import requests
import PyPDF2
import time

start_time = time.time()

# Variables generales
path = '/home/jandres/Escritorio/Tesis/PaginasWeb/sources/'
path_2 = '/home/jandres/Escritorio/Tesis/PaginasWeb/SITES/sources/'
nombres_archivos = []
lista_paginas = []
lista_paginas_2 = []
enlaces_politicas_finales = []

NUM_BROWSERS = 1
# Lista de sitios
# Para cuando los enlaces no tiene politica de privacidad en su pagina principal
# https://www.google.com/search?channel=fs&client=ubuntu&q=site%253A
# EJEMPLO
# https://www.google.com/search?channel=fs&client=ubuntu&q=site%3Ahttps://www.ups.edu.ec+politicas+privacidad
# CARGAR LOS ENLACES DE UN ARCHIVO

sitios = [
    # Enlaces nuevos
    "https://alige.com.mx/",
    "https://www.mapfre.com.mx/particulares/",
    "https://www.super.mx/",
    "https://ucad.edu.mx/",
    "https://colegioviktorfrankl.edu.mx/",
    "https://www.victoriaschool.edu.mx/",
    "https://www.colegiowilliams.edu.mx/",
    "https://www.liceo.edu.mx/rosemont/",
    "https://tefio.mx/",
    "https://enrefi.com.mx/",
    "https://www.banamex.com/",
    "https://www.santander.com.mx/",
    "https://www.bb.com.mx/webcenter/portal/BanBajio/home?_afrRedirect=4349461730411580",
    "https://invex.com/",
    "https://www.monex.com.mx/portal/",
    "https://www.bancoazteca.com.mx/",
    "https://www.multiva.com.mx/",
    "https://www.banregio.com/",

]
"""
"https:// utpl.edu.ec/",
    "https://www.utm.edu.ec/",
    "https://hospitalvozandes.com/",
    "https://www.hospitalvernaza.med.ec/",
    "https://secretariademovilidad.quito.gob.ec/",
    "https://www.bce.fin.ec/",
    "https://www.seguroscondor.com/inicio/",
    "https://latinaseguros.com.ec/",
    "https://www.orienteseguros.com/",
    "https://sweadenseguros.com/",
    "https://www.expreso.ec/",
    "https://www.portoviejo.gob.ec/",
    "https://amasegurosamerica.ec/",
    "https://equivida.com/",
    "https://www.coop23dejulio.fin.ec/",
    "https://www.epn.edu.ec/",
    "https://cooperativacotocollao.fin.ec/",
    "https://www.cooperativaambato.fin.ec/",
    "https://www.fondvida.fin.ec/",
    "https://aseguradoradelsur.com/home/",
    "https://www.generali.com.ec/",
    "https://www.fcaq.k12.ec/",
    "https://www.ups.edu.ec",
    "https://www.extra.ec/",
    "https://www.pichincha.com/portal/",
    "https://segurosconstitucion.com.ec/",
    "https://ambato.gob.ec/",
    "https://portal.compraspublicas.gob.ec/sercop/",
    "http://www.eeq.com.ec:8080/",
    "https://www.gamavision.com.ec/",
    "https://www.ford.com.ec/",
    "https://www.udla.edu.ec/",
    "https://www.produbanco.com.ec/",
    "https://www.puce.edu.ec/",
    "https://www.segurosalianza.com/",
    "https://www.futbolecuador.com/",
    "https://www.saludsa.com/",
    "https://www.fiscalia.gob.ec/",
    "https://www.ute.edu.ec/",
    "https://www.espol.edu.ec/",
    "https://www.usfq.edu.ec/",
    "https://uti.edu.ec/",
    "https://www.uotavalo.edu.ec/",
    "https://uisek.edu.ec/",
    "https://www.uide.edu.ec/",
    "https://uisrael.edu.ec/",
    "https://www.ministeriodegobierno.gob.ec/",
    "https://www.inclusion.gob.ec/",
    "https://www.salud.gob.ec/",
    "https://www.telecomunicaciones.gob.ec/",
    "https://educacion.gob.ec/",
    "https://www.agricultura.gob.ec/",
    "https://www.trabajo.gob.ec/",
    "https://www.defensa.gob.ec/",
    "https://www.hospitalmetropolitano.org/",
    "https://www.registrocivil.gob.ec/",
    "https://www.sri.gob.ec/web/intersri/home/",
    "https://www.cnelep.gob.ec/",
    "https://www.bancoamazonas.com/",
    "https://www.bolivariano.com/",
    "https://www.citi.com/icg/sa/latam/ecuador/",
    "https://www.bcmanabi.com/",
    "https://www.d-miro.com/",
    "https://www.bancoguayaquil.com/",
    "https://www.bancomachala.com/",
    "https://www.bancodelaustro.com/",
    "https://www.bancodelpacifico.com/personas/inicio/",
    "https://www.bancodesarrollo.fin.ec/",
    "https://www.bgr.com.ec/",
    "https://www.bancointernacional.com.ec/",
    "https://finca.ec/",
    "https://www.bancoprocredit.com.ec/",
    "https://www.banco-solidario.com/",
    "https://www.visionfund.ec/",
    "https://www.dinersclub.com.ec/",
    "https://bde.fin.ec/",
    "https://www.banecuador.fin.ec/",
    "https://www.biess.fin.ec/inicio",
    "https://www.cooprogreso.fin.ec/",
    "https://www.cpn.fin.ec/",
    "https://www.alianzadelvalle.fin.ec/",
    "https://www.cooprio.fin.ec/info/index.php/es/",
    "https://coacpuellaro.fin.ec/",
    "https://copedromoncayo.fin.ec/",
    "https://www.aig.com.ec/",
    "https://www.chubb.com/ec-es/",
    "https://www.bmicos.com/ecuador/",
    "https://www.bupasalud.com.ec/",
    "https://www.zurichseguros.com.ec/es-ec/inicio",
    "https://www.segurosinteroceanica.com/",
    "https://www.libertyseguros.ec/",
    "https://www.mapfre.com.ec/",
    "https://www.palig.com/es/ecuador/",
    "https://segurosunidos.ec/",
    "https://www.confianza.com.ec/",
    "https://segurosequinoccial.com/",
    "https://www.coface.com.ec/",
    "https://www.confiamed.com/web/",
    "https://humana.med.ec/",
    "https://plusmedical.com.ec/",
    "https://www.liceointernacional.edu.ec/",
    "https://colegiomenor.edu.ec/es/",
    "https://sekquito.com/",
    "https://www.primicias.ec/",
    "https://www.elcomercio.com/",
    "https://www.eluniverso.com/",
    "https://www.lahora.com.ec/",
    "https://www.teleamazonas.com/",
    "https://www.rts.com.ec/",
    "https://www.plusvalia.com/",
    "https://www.multitrabajos.com/",
    "https://www.netlife.ec/",
    "https://www.deprati.com.ec/",
    "https://www.loteria.com.ec/#/loteria/inicio/",
    "https://www.mercadolibre.com.ec/",
    "https://www.enkador.com/",
    "https://www.computrabajo.com.ec/",
    "https://xhateaec.com/",
    "https://ecuador.patiotuerca.com/",
    "https://www.frecuento.com/",
    "https://www.dialcentro.com.ec/",
    "https://studiofutbol.com.ec/",
    "https://www.axeso5.com/",
    "http://www.forosecuador.ec/index.php/",
    "https://radios.com.ec/",
    "https://www.chevrolet.com.ec/",
    "https://www.kia.com/ec/main.html/",
    "https://www.nissan.com.ec/",
    "https://www.mazda.com.ec/",
    "https://www.hyundai.com.ec/",
    "https://www.despegar.com.ec/",
    "https://www.tia.com.ec/",
    "https://www.movistar.com.ec/",
    "https://www.claro.com.ec/personas/",
    "https://www.cnt.com.ec/",
    "http://www.fmc.com.ec/",
    "https://fundacionvistaparatodos.com.ec/",
    "https://www.gob.ec/gaddmq/",
    "https://www.guayaquil.gob.ec/",
    "https://www.cuenca.gob.ec/",
    "https://www.ibarra.gob.ec/site/",
    "https://ruminahui.gob.ec/",
    "https://www.latacunga.gob.ec/es/",
    "https://www.loja.gob.ec/",
    "https://www.iess.gob.ec",
    "https://sanjuandedios.ec/hospitalquito/",
    "http://www.novaclinicasantacecilia.com/",
    "https://www.andalucia.fin.ec/",
    "https://www.atuntaqui.fin.ec/",
    "https://www.asis.fin.ec/",
    "https://www.cooptulcan.fin.ec/",
    "https://esmeraldas.gob.ec/",
    "https://www.funcionjudicial.gob.ec/",
    "https://www.quito.gob.ec/",
    "https://municipiocayambe.gob.ec/",
    "https://www.aguaquito.gob.ec/",
    "http://www.amt.gob.ec/",
    "https://www.ant.gob.ec/",
    "https://www.utmachala.edu.ec/portalwp/",
    "https://www.espe.edu.ec",
    "https://www.hospitaldelosvalles.com/",
    "https://www.hospitalpadrecarollo.org/",
    "https://hospitalalcivar.com/",
    "https://www.clinicacotocollaoec.com/",
    "https://www.clinicasantamaria.ec/",
    "https://clinicamilenium.com.ec/",
    "https://www.bancocapital.com/",
    "https://www.bancocoopnacional.com/",
    "https://www.bancodeloja.fin.ec/",
    "https://www.litoral.fin.ec/",
    "https://www.delbank.fin.ec/",
    "https://www.cfn.fin.ec/",
    "https://www.cfn.fin.ec/fondo-nacional-de-garantia/",
    "https://www.jep.coop/",
    "https://www.29deoctubre.fin.ec/",
    "https://ecuasuiza.ec/",
    "https://hispanadeseguros.com/inicio/",
    "https://seguroslaunion.com/",
    "https://segurosdelpichincha.com/",
    "https://www.segurossucre.fin.ec/",
    "https://www.seguroscolon.com/",
    "https://vazseguros.com/",
    "http://alfamedical.co.kr/?lang=es/",
    "https://asisken.com/",
    "https://www.bluecard.com.ec/inicio/",
    "https://www.ecuasanitas.com/",
    "http://www.cruzblanca.com.ec/",
    "https://www.mediken.com.ec/home.aspx/",
    "https://planvital.ec/",
    "https://www.privilegio.med.ec/",
    "https://condamine.edu.ec/",
    "https://www.shakespeare.edu.ec/",
    "https://www.colegioterranova.edu.ec/",
    "https://www.intisana.com/",
    "https://www.caq.edu.ec/",
    "http://www.einstein.k12.ec/es/",
    "https://www.ang.edu.ec/",
    "https://alemanhumboldt.edu.ec/guayaquil/",
    "https://interamericano.ec/",
    "https://liceolosandes.edu.ec/",
    "https://www.liceocampoverde.edu.ec/campoverde_v2/#/home/",
    "https://colegiolospinos.ec/",
    "https://www.csgabriel.edu.ec/",
    "https://spellman.edu.ec/",
    "https://www.cotopaxi.k12.ec/es",
    "https://www.miraflores.edu.ec/",
    "https://www.cenu.edu.ec/",
    "https://www.colegioamericano.edu.ec/",
    "https://elmercurio.com.ec/",
    "https://www.ecuavisa.com/",
    "https://www.tctelevision.com/",
    "https://www.eldiario.ec/",

"""

# Creacion de la base de datos
remove('/home/jandres/Escritorio/Tesis/base.sqlite')
conn = sqlite3.connect('/home/jandres/Escritorio/Tesis/base.sqlite', timeout=100)
cur = conn.cursor()
# Creacion del reporte final
cur.execute(
    " CREATE TABLE REPORTE_FINAL AS SELECT '' AS 'ROW_ID', '' AS 'Entity URL Full', '' AS 'Entity URL', '' AS 'Policy URL', '' AS 'Observaciones' ")
cur.executemany("INSERT INTO REPORTE_FINAL ([Entity URL Full]) VALUES (?)", [(x,) for x in sitios])
cur.executescript(""" 
DELETE FROM REPORTE_FINAL WHERE ROWID = 1;
UPDATE REPORTE_FINAL SET ROW_ID = ROWID-1;
CREATE TABLE ENLACES_TOTALES_INICIALES_SIN_FILTRO AS SELECT ROW_ID FROM REPORTE_FINAL;
CREATE TABLE ENLACES_TOTALES_INICIALES_SIN_GOOGLE AS SELECT ROW_ID FROM REPORTE_FINAL;
CREATE TABLE ENLACES_TOTALES_INICIALES_GOOGLE AS SELECT ROW_ID FROM REPORTE_FINAL;
                  """)
conn.commit()

for i in range(0, len(sitios)):
    # Se crea una subcadena para obtener el nombre de los archivos .html
    sub_sites = sitios[i][sitios[i].find('//') + 2:len(sitios[i])][
                0:sitios[i][sitios[i].find('//') + 2:len(sitios[i])].find('/')]
    aux_bol = sub_sites.startswith('www')
    if aux_bol == True:
        sub_sites = sub_sites[sub_sites.find('www.') + 4:len(sub_sites)]
        nombres_archivos.append(sub_sites)
    else:
        nombres_archivos.append(sub_sites)
    # SE ingresa el Entity URL a la tabla de reporte
    nombres_archivos_sql = "".join(nombres_archivos[i])
    cur.execute(
        "UPDATE REPORTE_FINAL SET [Entity URL] = '" + nombres_archivos_sql + "' WHERE [Entity URL Full] LIKE '%" + nombres_archivos_sql + "%'")
    conn.commit()

# Se limpia la carpeta en donde se van a descargar las paginas web
archivos_borrados = glob.glob('/home/jandres/Escritorio/Tesis/PaginasWeb/sources/*.html')
for i in archivos_borrados:
    remove(i)
# browser_params = [BrowserParams(display_mode="native") for _ in range(NUM_BROWSERS)]
manager_params = ManagerParams(num_browsers=NUM_BROWSERS)
browser_params = [BrowserParams(display_mode="headless") for _ in range(NUM_BROWSERS)]
manager_params.data_directory = Path("/home/jandres/Escritorio/Tesis/PaginasWeb")
manager_params.log_path = Path("/home/jandres/Escritorio/Tesis/PaginasWeb/openwpm.log")

with TaskManager(manager_params, browser_params,
                 SQLiteStorageProvider(Path("/home/jandres/Escritorio/Tesis/PaginasWeb/crawl-database.sqlite")),
                 None, ) as manager:
    # Visits the sites
    for index, site in enumerate(sitios):
        # Paralelizar sitios en todos los navegadores establecidos anteriormente
        command_sequence = CommandSequence(
            site,
            site_rank=index,
        )
        # Visita la pagina
        command_sequence.append_command(GetCommand(url=site, sleep=3), timeout=30)
        # Descarga el codigo fuente de la sitio especificado
        command_sequence.append_command(DumpPageSourceCommand(suffix='__' + nombres_archivos[index]), timeout=30)
        # Eche un vistazo a custom_command.py para ver cómo implementar su propio comando
        command_sequence.append_command(LinkCountingCommand())
        # Se ejecuta los comandos
        manager.execute_command_sequence(command_sequence)

end_time = time.time()
time_total_1 = end_time - start_time

start_time = time.time()

with os.scandir(path) as lista_paginas:
    lista_paginas = [lista_paginas.name for lista_paginas in lista_paginas if lista_paginas.is_file()]

nombre_lista_paginas_final = []
yx = 0
# Se recorre la lista de paginas que se descargaron porque pueda que no se descargue alguna pagina web
for j in range(0, len(lista_paginas)):
    # Recorre la carpeta para obtener la ruta de las paginas web
    nombre_lista_paginas = lista_paginas[j][lista_paginas[j].find('__') + 2:len(lista_paginas[j])]
    nombre_tabla_enlaces = nombre_lista_paginas[0:nombre_lista_paginas.find('.')]
    nombre_tabla_enlaces = "".join(nombre_tabla_enlaces).replace('-', '')
    nombre_lista_paginas_final.append(nombre_lista_paginas)

    # Se elimina el csv para que en la siguiente vuelta este limpia con los nuevos enlaces de la siguiente pagina
    remove('/home/jandres/Escritorio/Tesis/salida_enlaces.csv')
    remove('/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv')

    # Se crea un .csv para guardar los enlaces deseados
    with open("/home/jandres/Escritorio/Tesis/salida_enlaces.csv", "a+") as enlaces_politicas:
        # Nombre de la columna en tabla
        enlaces_politicas.write("enlaces")

    # Se crea un .csv para guardar los enlaces finales deseados
    with open("/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv", "a+") as enlaces_finales_sql:
        # Nombre de la columna en tabla
        enlaces_finales_sql.write("ENLACES")

    # Validacion para leer el .html correcto
    with open(path + lista_paginas[j]) as archivos:
        soup = BeautifulSoup(archivos, "html.parser")
        etiquetas = soup.find_all('a')
        for n in etiquetas:
            with open("/home/jandres/Escritorio/Tesis/salida_enlaces.csv", "a+") as enlaces_politicas:
                # Formato general
                parametro_enlace = n.attrs.get('href')
                # Caso espercial para el comercio
                parametro_enlace_elcomercio = n.attrs.get('data-href')  # Caso especial el comercio
                enlaces_politicas.write('\n % s' % parametro_enlace)
                enlaces_politicas.write('\n % s' % parametro_enlace_elcomercio)

    # Dio problemas al leer el csv aqui la solucion https://stackoverflow.com/questions/18039057/python-pandas-error-tokenizing-data
    # Version de pandas 1.4.1
    tabla_enlaces = pd.read_csv('/home/jandres/Escritorio/Tesis/salida_enlaces.csv', on_bad_lines='skip')
    tabla_enlaces.to_sql('tabla_enlaces', conn, if_exists='replace', index=False, index_label='enlaces',
                         chunksize=10000)
    conn.commit()
    # Se procesa la informacion de la tabla principal
    cur.execute('''UPDATE tabla_enlaces SET enlaces = TRIM(enlaces)''')

    # Se obtiene los enlaces finales, se crea la variable para que en la siguiente iteracion quede limpia
    enlaces_politicas = []
    enlaces_finales = []
    consultas = [
        " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%priv%' ",
        " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%personales%'",
        " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%politica%' ",
        " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%terminos%' ",
    ]
    # Pasa varias instrucciones a la vez en base de datos
    for k in range(0, len(consultas)):
        # Especificar diferencia entre fetchone y fetchall para descubrir la solucion
        resultado_consultas = cur.execute(consultas[k]).fetchall()
        if resultado_consultas is not None:
            for l in resultado_consultas:
                separador = ""
                resultado_consultas = separador.join(str(resultado_consultas) for resultado_consultas in l)
                enlaces_politicas.append(resultado_consultas)
                # Se elimina los enlaces repetidos
                for m in enlaces_politicas:
                    if m not in enlaces_finales:
                        enlaces_finales.append(m)
                        for fila in range(0, len(enlaces_finales)):
                            with open("/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv",
                                      "a+") as enlaces_finales_sql:
                                enlaces_finales_sql.write('\n % s' % enlaces_finales[fila])

    tabla_enlaces_finales = pd.read_csv('/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv',
                                        on_bad_lines='skip')
    conn.commit()
    tabla_enlaces_finales.to_sql('tabla_enlaces_finales', conn, if_exists='replace', index=False, index_label='ENLACES',
                                 chunksize=10000, method='multi')

    # Se obtiene todos los enlaces de la pagina web actual
    cur.executescript(""" 
    DELETE FROM tabla_enlaces WHERE enlaces = 'None';
    DELETE FROM tabla_enlaces WHERE enlaces = '#';
    CREATE TABLE tabla_enlaces_finales_TMP AS SELECT * FROM tabla_enlaces_finales;
    DROP TABLE tabla_enlaces_finales;
    ALTER TABLE tabla_enlaces_finales_TMP RENAME TO tabla_enlaces_finales;
    ALTER TABLE tabla_enlaces_finales ADD COLUMN ROW_ID;
    UPDATE tabla_enlaces_finales SET ROW_ID = ROWID;
    """)
    # Ingreso de enlaces para el rePORTE SIN FILTRO
    yx = yx + 1
    consulta = "ALTER TABLE ENLACES_TOTALES_INICIALES_SIN_FILTRO ADD COLUMN [" + nombre_tabla_enlaces + "_" + str(
        yx) + "]"
    cur.execute(consulta)
    # Ingreso de enlaces para el reporte SIN GOOGLE
    consulta = "ALTER TABLE ENLACES_TOTALES_INICIALES_SIN_GOOGLE ADD COLUMN [" + nombre_tabla_enlaces + "_" + str(
        yx) + "]"
    cur.execute(consulta)
    # Ingreso de enlaces para el reporte SIN GOOGLE
    consulta = "ALTER TABLE ENLACES_TOTALES_INICIALES_GOOGLE ADD COLUMN [" + nombre_tabla_enlaces + "_" + str(
        yx) + "]"
    cur.execute(consulta)
    conn.commit()
    consulta = 'UPDATE ENLACES_TOTALES_INICIALES_SIN_FILTRO SET  [' + nombre_tabla_enlaces + '_' + str(
        yx) + ']  = (SELECT enlaces FROM tabla_enlaces_finales WHERE ENLACES_TOTALES_INICIALES_SIN_FILTRO.ROW_ID = tabla_enlaces_finales.ROW_ID)'
    cur.execute(consulta)
    conn.commit()

    # Procesamiento de informacion de la tabla de enlaces finales

    cur.executescript(""" 
    CREATE TABLE tabla_enlaces_finales_politicas AS SELECT DISTINCT(TRIM(ENLACES)) AS 'Policy_URL' FROM tabla_enlaces_finales;
    ALTER TABLE tabla_enlaces_finales_politicas ADD COLUMN ROW_ID;
    UPDATE tabla_enlaces_finales_politicas SET ROW_ID = ROWID;
    ALTER TABLE tabla_enlaces_finales_politicas ADD COLUMN URL_PARTE_FINAL;
    ALTER TABLE tabla_enlaces_finales_politicas ADD COLUMN CARACTER;
    UPDATE tabla_enlaces_finales_politicas SET CARACTER = substr(Policy_URL,-1);
    UPDATE tabla_enlaces_finales_politicas SET Policy_URL = trim(substr(Policy_URL,1,length(Policy_URL)-1)) WHERE CARACTER = '/';
    UPDATE tabla_enlaces_finales_politicas SET URL_PARTE_FINAL = replace(Policy_URL, rtrim(Policy_URL, replace(Policy_URL,'/','')),'');
    DELETE FROM tabla_enlaces_finales_politicas WHERE Policy_URL LIKE '%accounts.google%';
    DELETE FROM tabla_enlaces_finales_politicas WHERE Policy_URL LIKE '%/search%';
    DELETE FROM tabla_enlaces_finales_politicas WHERE Policy_URL LIKE '%/advanced%';
    DELETE FROM tabla_enlaces_finales_politicas WHERE Policy_URL LIKE '%google%';
    DELETE FROM tabla_enlaces_finales_politicas WHERE length(Policy_URL) > 1000;

    """)
    '    UPDATE tabla_enlaces_finales_politicas SET Policy_URL = lower(Policy_URL); '

    consultas_2 = [
        "SELECT Policy_URL FROM tabla_enlaces_finales_politicas WHERE URL_PARTE_FINAL LIKE '%privac%' ",
        "SELECT Policy_URL FROM tabla_enlaces_finales_politicas WHERE URL_PARTE_FINAL LIKE'%personales%'",
        "SELECT Policy_URL FROM tabla_enlaces_finales_politicas WHERE URL_PARTE_FINAL LIKE '%politica%'",
        "SELECT Policy_URL FROM tabla_enlaces_finales_politicas WHERE URL_PARTE_FINAL LIKE'%terminos%' ",

    ]
    # Se obtiene los enlaces finales, se crea la variable para que en la siguiente iteracion quede limpia
    enlaces_politicas = []
    enlaces_finales = []
    enlaces_no_index = []
    # Pasa varias instrucciones a la vez en base de datos
    for k in range(0, len(consultas_2)):
        # Especificar diferencia entre fetchone y fetchall para descubrir la solucion
        resultado_consultas_final = cur.execute(consultas_2[k]).fetchall()
        conn.commit()
        if resultado_consultas_final is not None:
            for l in resultado_consultas_final:
                separador = ""
                resultado_consultas_final = separador.join(
                    str(resultado_consultas_final) for resultado_consultas_final in l)

                consulta = "UPDATE ENLACES_TOTALES_INICIALES_SIN_GOOGLE SET  [" + nombre_tabla_enlaces + "_" + str(
                    yx) + "]  = '" + resultado_consultas_final + "' WHERE ROW_ID = (SELECT MIN(ROWID) FROM ENLACES_TOTALES_INICIALES_SIN_GOOGLE WHERE [" + nombre_tabla_enlaces + "_" + str(
                    yx) + "] IS NULL)"
                cur.execute(consulta)
                conn.commit()
                enlaces_politicas.append(resultado_consultas_final)
                # Se elimina los enlaces repetidos
                for m in enlaces_politicas:
                    if m not in enlaces_finales:
                        enlaces_finales.append(m)
    # Se descarga la pagina con sites: en google cuando no encuentra la politica en la pagina principal
    pagina_no_index = []
    if len(enlaces_finales) == 0:
        nombre_lista_paginas_sql = "".join(nombre_lista_paginas)
        nombre_lista_paginas_sql = nombre_lista_paginas_sql.replace(".html", "")
        nombre_lista_paginas_sql = nombre_lista_paginas_sql.replace("sites__", "")

        pagina_no_index = cur.execute(
            "SELECT [Entity URL Full] FROM REPORTE_FINAL WHERE [Entity URL Full] LIKE '%" + nombre_lista_paginas_sql + "%'").fetchone()

        pagina_no_index = "https://www.google.com/search?channel=fs&client=ubuntu&q=site%3A" + pagina_no_index[
            0] + "+politicas+de+privacidad"

        enlaces_no_index.append(pagina_no_index)

        # Cambio de ruta para los archivos que se descargan con google
        manager_params.data_directory = Path("/home/jandres/Escritorio/Tesis/PaginasWeb/SITES")
        # Se limpia la carpeta en donde se van a descargar las paginas web de las politicas
        archivos_borrados = glob.glob('/home/jandres/Escritorio/Tesis/PaginasWeb/SITES/sources/*.html')
        for z in archivos_borrados:
            remove(z)

        with TaskManager(manager_params, browser_params,
                         SQLiteStorageProvider(Path("/home/jandres/Escritorio/Tesis/PaginasWeb/crawl-database.sqlite")),
                         None, ) as manager:
            # Visits the sites
            for index, site in enumerate(enlaces_no_index):
                # Paralelizar sitios en todos los navegadores establecidos anteriormente
                command_sequence = CommandSequence(
                    site,
                    site_rank=index,
                )
                # Visita la pagina
                command_sequence.append_command(GetCommand(url=site, sleep=3), timeout=60)
                # Descarga el codigo fuente de la sitio especificado
                command_sequence.append_command(DumpPageSourceCommand(suffix='__sites__' + nombre_lista_paginas_sql),
                                                timeout=30)
                # Eche un vistazo a custom_command.py para ver cómo implementar su propio comando
                command_sequence.append_command(LinkCountingCommand())
                # Se ejecuta los comandos
                manager.execute_command_sequence(command_sequence)

        # Lee los archivos descargados y los lista
        with os.scandir(path_2) as lista_paginas_2:
            lista_paginas_2 = [lista_paginas_2.name for lista_paginas_2 in lista_paginas_2 if lista_paginas_2.is_file()]

        # Se usa expresiones regulares para buscar la pagina de google sites que se descargo
        patron = re.compile(f"sites__{nombre_lista_paginas_sql}")
        nueva_lista_pagina_no_ind = []
        for archivo in lista_paginas_2:
            if patron.search(archivo):
                nueva_lista_pagina_no_ind.append(archivo)

        # Se elimina el csv para que en la siguiente vuelta este limpia con los nuevos enlaces de la siguiente pagina
        remove('/home/jandres/Escritorio/Tesis/salida_enlaces.csv')
        remove('/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv')

        # Se crea un .csv para guardar los enlaces deseados
        with open("/home/jandres/Escritorio/Tesis/salida_enlaces.csv", "a+") as enlaces_politicas:
            # Nombre de la columna en tabla
            enlaces_politicas.write("enlaces")

        # Se crea un .csv para guardar los enlaces finales deseados
        with open("/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv", "a+") as enlaces_finales_sql:
            # Nombre de la columna en tabla
            enlaces_finales_sql.write("ENLACES")

        # Validacion para leer el .html correcto
        with open(path_2 + nueva_lista_pagina_no_ind[0]) as archivos:
            soup = BeautifulSoup(archivos, "html.parser")
            etiquetas = soup.find_all('a')
            for n in etiquetas:
                with open("/home/jandres/Escritorio/Tesis/salida_enlaces.csv", "a+") as enlaces_politicas:
                    # Formato general
                    parametro_enlace = n.attrs.get('href')
                    # Caso espercial para el comercio
                    parametro_enlace_elcomercio = n.attrs.get('data-href')  # Caso especial el comercio
                    enlaces_politicas.write('\n % s' % parametro_enlace)
                    enlaces_politicas.write('\n % s' % parametro_enlace_elcomercio)

        # Version de pandas 1.4.1
        tabla_enlaces = pd.read_csv('/home/jandres/Escritorio/Tesis/salida_enlaces.csv', on_bad_lines='skip')

        cur.execute('''DROP TABLE tabla_enlaces ''')

        tabla_enlaces.to_sql('tabla_enlaces', conn, if_exists='replace', index=False, index_label='enlaces',
                             chunksize=10000)
        conn.commit()

        # Se procesa la informacion de la tabla principal

        # Se obtiene los enlaces finales, se crea la variable para que en la siguiente iteracion quede limpia
        enlaces_politicas = []
        enlaces_finales = []

        consultas = [
            # Filtro nivel 1
            " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%" + nombre_lista_paginas_sql + "%' AND enlaces LIKE '%priv%' ",
            " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%" + nombre_lista_paginas_sql + "%' AND enlaces LIKE '%personales%' ",
            " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%" + nombre_lista_paginas_sql + "%' AND enlaces LIKE '%politica%' ",
            " SELECT trim(DISTINCT(enlaces)) FROM tabla_enlaces WHERE enlaces LIKE '%" + nombre_lista_paginas_sql + "%' AND enlaces LIKE '%terminos%' ",

        ]

        # Pasa varias instrucciones a la vez en base de datos
        for k in range(0, len(consultas)):
            # Especificar diferencia entre fetchone y fetchall para descubrir la solucion
            resultado_consultas = cur.execute(consultas[k]).fetchall()
            if resultado_consultas is not None:
                for l in resultado_consultas:
                    separador = ""
                    resultado_consultas = separador.join(str(resultado_consultas) for resultado_consultas in l)
                    enlaces_politicas.append(resultado_consultas)
                    # Se elimina los enlaces repetidos
                    for m in enlaces_politicas:
                        if m not in enlaces_finales:
                            enlaces_finales.append(m)
                            for fila in range(0, len(enlaces_finales)):
                                with open("/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv",
                                          "a+") as enlaces_finales_sql:
                                    enlaces_finales_sql.write('\n % s' % enlaces_finales[fila])

        tabla_enlaces_finales = pd.read_csv('/home/jandres/Escritorio/Tesis/salida_enlaces_finales.csv',
                                            on_bad_lines='skip')
        tabla_enlaces_finales.to_sql('tabla_enlaces_finales', conn, if_exists='replace', index=False,
                                     index_label='ENLACES',
                                     chunksize=10000, method='multi')
        conn.commit()

        cur.executescript(""" 
        CREATE TABLE tabla_enlaces_finales_politicas_2 AS SELECT DISTINCT(TRIM(ENLACES)) AS 'Policy_URL' FROM tabla_enlaces_finales;
        ALTER TABLE tabla_enlaces_finales_politicas_2 ADD COLUMN ROW_ID;
        UPDATE tabla_enlaces_finales_politicas_2 SET ROW_ID = ROWID;
        ALTER TABLE tabla_enlaces_finales_politicas_2 ADD COLUMN URL_PARTE_FINAL;
        ALTER TABLE tabla_enlaces_finales_politicas_2 ADD COLUMN CARACTER;
        UPDATE tabla_enlaces_finales_politicas_2 SET CARACTER = substr(Policy_URL,-1);
        UPDATE tabla_enlaces_finales_politicas_2 SET Policy_URL = trim(substr(Policy_URL,1,length(Policy_URL)-1)) WHERE CARACTER = '/';
        UPDATE tabla_enlaces_finales_politicas_2 SET URL_PARTE_FINAL = replace(Policy_URL, rtrim(Policy_URL, replace(Policy_URL,'/','')),'');
        DELETE FROM tabla_enlaces_finales_politicas_2 WHERE Policy_URL LIKE '%accounts.google%';
        DELETE FROM tabla_enlaces_finales_politicas_2 WHERE Policy_URL LIKE '%/search%';
        DELETE FROM tabla_enlaces_finales_politicas_2 WHERE Policy_URL LIKE '%/advanced%';
        DELETE FROM tabla_enlaces_finales_politicas_2 WHERE Policy_URL LIKE '%google%';
     
        """)

        '   UPDATE tabla_enlaces_finales_politicas_2 SET Policy_URL = lower(Policy_URL); '

        consultas_2 = [
            # Filtro nivel 1
            "SELECT Policy_URL FROM tabla_enlaces_finales_politicas_2 WHERE URL_PARTE_FINAL LIKE '%privac%' ",
            "SELECT Policy_URL FROM tabla_enlaces_finales_politicas_2 WHERE URL_PARTE_FINAL LIKE'%personales%' ",
            "SELECT Policy_URL FROM tabla_enlaces_finales_politicas_2 WHERE URL_PARTE_FINAL LIKE '%politica%'",
            "SELECT Policy_URL FROM tabla_enlaces_finales_politicas_2 WHERE URL_PARTE_FINAL LIKE'%terminos%' ",
        ]

        # Se obtiene los enlaces finales, se crea la variable para que en la siguiente iteracion quede limpia
        enlaces_politicas = []
        enlaces_finales = []
        enlaces_no_index = []
        # Pasa varias instrucciones a la vez en base de datos
        for k in range(0, len(consultas_2)):
            # Especificar diferencia entre fetchone y fetchall para descubrir la solucion
            resultado_consultas_final = cur.execute(consultas_2[k]).fetchall()
            if resultado_consultas_final is not None:
                for l in resultado_consultas_final:
                    separador = ""
                    resultado_consultas_final = separador.join(
                        str(resultado_consultas_final) for resultado_consultas_final in l)

                    consulta = "UPDATE ENLACES_TOTALES_INICIALES_GOOGLE SET  [" + nombre_tabla_enlaces + "_" + str(
                        yx) + "]  = '" + resultado_consultas_final + "' WHERE ROW_ID = (SELECT MIN(ROWID) FROM ENLACES_TOTALES_INICIALES_GOOGLE WHERE [" + nombre_tabla_enlaces + "_" + str(
                        yx) + "] IS NULL)"
                    cur.execute(consulta)
                    conn.commit()

                    enlaces_politicas.append(resultado_consultas_final)
                    # Se elimina los enlaces repetidos
                    for m in enlaces_politicas:
                        if m not in enlaces_finales:
                            enlaces_finales.append(m)

        if len(enlaces_finales) != 0:
            enlaces_finales = enlaces_finales[0]
            enlaces_politicas_finales.append(enlaces_finales)

        # AQUI SE DEBERIA ACTUALIZAR EL REPORTE FINAL
        else:
            cur.execute(
                "UPDATE REPORTE_FINAL SET Observaciones = 'NO SE PUDO ENCONTRAR LA POLITICA DE PRIVACIDAD' WHERE [Entity URL Full] LIKE '%" + nombre_lista_paginas_sql + "%'")

    else:
        aux_bol = enlaces_finales[0].startswith('https')
        if aux_bol == False:
            # Se transforma a str la palabra de la lista para reemplazar lo que no se desea y crear un nuevo enlace
            nombres_especiales = "".join(nombre_lista_paginas_final[j])
            nombres_especiales = nombres_especiales.replace(".html", "")
            enlaces_finales = ["https://www." + nombres_especiales + "/" + enlaces_finales[0]]

        # Se obtiene el primer enlace
        enlaces_finales = enlaces_finales[0]
        enlaces_politicas_finales.append(enlaces_finales)

    # Copia de respaldo
    shutil.copy('/home/jandres/Escritorio/Tesis/base.sqlite', '/home/jandres/Escritorio/Tesis/data_base.sqlite')
    cur.execute("""DROP TABLE tabla_enlaces_finales_politicas""")
    cur.execute("""DROP TABLE IF EXISTS tabla_enlaces_finales_politicas_2""")

# Se cierra la conexion a la base
conn.commit()
conn.close()

end_time = time.time()
time_total_2 = end_time - start_time

start_time = time.time()

# Lee los archivos descargados y los lista
#### DESCARGA DE LA POLITICA ####
# Se obtiene el nombre final de los archivos de las politicas
nombres_archivos = []

for x in range(0, len(enlaces_politicas_finales)):
    # Se crea una subcadena para obtener el nombre de los archivos .html
    sub_sites = enlaces_politicas_finales[x][
                enlaces_politicas_finales[x].find('//') + 2:len(enlaces_politicas_finales[x])][
                0:enlaces_politicas_finales[x][
                  enlaces_politicas_finales[x].find('//') + 2:len(enlaces_politicas_finales[x])].find('/')]
    aux_bol = sub_sites.startswith('www')
    if aux_bol == True:
        sub_sites = sub_sites[sub_sites.find('www.') + 4:len(sub_sites)]
        nombres_archivos.append(sub_sites)
    else:
        nombres_archivos.append(sub_sites)

# browser_params = [BrowserParams(display_mode="native") for _ in range(NUM_BROWSERS)]
browser_params = [BrowserParams(display_mode="headless") for _ in range(NUM_BROWSERS)]
manager_params.data_directory = Path("/home/jandres/Escritorio/Tesis/Politicas/PaginasWeb")
manager_params.log_path = Path("/home/jandres/Escritorio/Tesis/Politicas/PaginasWeb/openwpm.log")

# Se limpia la carpeta en donde se van a descargar las paginas web de las politicas
archivos_borrados = glob.glob('/home/jandres/Escritorio/Tesis/Politicas/PaginasWeb/sources/*.html')
for z in archivos_borrados:
    remove(z)

with TaskManager(manager_params, browser_params,
                 SQLiteStorageProvider(Path("/home/jandres/Escritorio/Tesis/Politicas/PaginasWeb/crawl-data.sqlite")),
                 None, ) as manager:
    # Visits the sites
    for index, site in enumerate(enlaces_politicas_finales):
        # Paralelizar sitios en todos los navegadores establecidos anteriormente
        command_sequence = CommandSequence(
            site,
            site_rank=index,
        )
        # Visita la pagina
        command_sequence.append_command(GetCommand(url=site, sleep=3), timeout=60)
        # Descarga el codigo fuente de la sitio especificado
        command_sequence.append_command(DumpPageSourceCommand(suffix='__' + nombres_archivos[index]),
                                        timeout=30)
        # Eche un vistazo a custom_command.py para ver cómo implementar su propio comando
        command_sequence.append_command(LinkCountingCommand())
        # Se ejecuta los comandos
        manager.execute_command_sequence(command_sequence)

#### SE OBTIENE LA POLITICA ####
# Lee las politicas descargadas y los lista
path_politicas = "/home/jandres/Escritorio/Tesis/Politicas/PaginasWeb/sources/"
with os.scandir(path_politicas) as lista_paginas_politicas:
    lista_paginas_politicas = [lista_paginas_politicas.name for lista_paginas_politicas in lista_paginas_politicas if
                               lista_paginas_politicas.is_file()]

# Obtencion del texto plano de la politica
nombre_lista_paginas_final_politicas_priv = []
conn = sqlite3.connect('/home/jandres/Escritorio/Tesis/base.sqlite', timeout=10)
cur = conn.cursor()

end_time = time.time()
time_total_3 = end_time - start_time

start_time = time.time()

for y in range(0, len(lista_paginas_politicas)):

    # Recorre la carpeta para obtener la ruta de las paginas web
    nombre_lista_paginas_politicas_priv = lista_paginas_politicas[y][
                                          lista_paginas_politicas[y].find('__') + 2:len(lista_paginas_politicas[y])]
    nombre_lista_paginas_politicas_priv = "".join(nombre_lista_paginas_politicas_priv)
    nombre_lista_paginas_politicas_priv = nombre_lista_paginas_politicas_priv.replace('.html', '')
    nombre_lista_paginas_final_politicas_priv.append(nombre_lista_paginas_politicas_priv)

    with open(path_politicas + lista_paginas_politicas[y], "r") as archivo_final:
        contenido_pagina = archivo_final.read()
        soup_end = BeautifulSoup(contenido_pagina, 'html.parser')

    # Lectura de archivos PDF

    pdf_url = re.search(r'http.*\.pdf', contenido_pagina)
    try:
        if pdf_url:
            pdf_url = pdf_url.group(0)
            response = requests.get(pdf_url)

            with tempfile.NamedTemporaryFile(delete=True) as temp:
                temp.write(response.content)
                temp.seek(0)
                pdf_file = PyPDF2.PdfFileReader(temp)
                text = ''
                for page in pdf_file.pages:
                    text += page.extract_text()

            with open('/home/jandres/Escritorio/Tesis/Politicas/Archivos finales/' +
                      nombre_lista_paginas_final_politicas_priv[
                          y] + '.txt', "w") as f:
                f.write(text)
    except Exception as e:
        print('Error al procesar el archivo: ', pdf_url)
        print(e)

    # Se usa la subcadena de nombres de archivos y busca las etiquetas p y guarda la informacion en un archivo de texto
    with open('/home/jandres/Escritorio/Tesis/Politicas/Archivos finales/' + nombre_lista_paginas_final_politicas_priv[
        y] + '.txt', "w") as f:
        for datos_txt in soup_end.find_all('div'):
            suma_txt = datos_txt.get_text()
            f.writelines(suma_txt)

        for datos_txt_2 in soup_end.find_all('tr'):
            suma_txt_2 = datos_txt_2.get_text()
            f.writelines(suma_txt_2)

        for datos_txt_3 in soup_end.find_all('p'):
            suma_txt_3 = datos_txt_3.get_text()
            f.writelines(suma_txt_3)

    # Actualizacion de la tabla final
    cur.execute("UPDATE REPORTE_FINAL SET Observaciones = 'POLITICA OBTENIDA' WHERE [Entity URL Full] LIKE '%" +
                nombre_lista_paginas_final_politicas_priv[y] + "%'")
    conn.commit()

end_time = time.time()
time_total_4 = end_time - start_time

print('El tiempo de ejecucion es: ', time_total_1, 'segundos')
print('El tiempo de ejecucion es: ', time_total_2, 'segundos')
print('El tiempo de ejecucion es: ', time_total_3, 'segundos')
print('El tiempo de ejecucion es: ', time_total_4, 'segundos')
