import pandas as pd
import logging
import datetime as dt

## Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('logs.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

# create df_cultural
def create_df_cultural(df_museos, df_cines, df_bibliotecas):
    
    # rename df_museos columns
    df_museos = df_museos.drop(['Observaciones', 'subcategoria', 'piso', 
        'cod_area', 'Latitud', 'Longitud', 'TipoLatitudLongitud',
        'Info_adicional', 'jurisdiccion', 'año_inauguracion',
        'IDSInCA'], axis=1)

    df_museos.columns = ['Cod_Loc', 'Id_Provincia', 'Id_Departamento', 'Categoria', 
        'Provincia', 'Localidad', 'Nombre', 'Domicilio', 'Cod_Postal', 'Telefono', 'Mail', 'Web', 'Fuente']

    # rename df_cines columns
    df_cines = df_cines.drop(['Observaciones', 'Piso', "Departamento",
        'cod_area', 'Latitud', 'Longitud', 'TipoLatitudLongitud',
        'Información adicional', 'Latitud', 'Longitud', 'TipoLatitudLongitud',
        'tipo_gestion', 'Pantallas', 'Butacas', 'espacio_INCAA',
        'año_actualizacion'], axis=1)

    df_cines.columns = ['Cod_Loc', 'Id_Provincia', 'Id_Departamento', 'Categoria', 
        'Provincia', 'Localidad', 'Nombre', 'Domicilio', 'Cod_Postal', 'Telefono', 'Mail', 'Web', 'Fuente']

    # rename df_bibliotecas columns
    df_bibliotecas = df_bibliotecas.drop(['Observacion', 'Subcategoria', 'Piso', "Departamento",
        'Información adicional', 'Latitud', 'Longitud', 'TipoLatitudLongitud', 'Cod_tel',
        'Tipo_gestion', 'año_inicio',
        'Año_actualizacion'], axis=1)

    df_bibliotecas.columns = ['Cod_Loc', 'Id_Provincia', 'Id_Departamento', 'Categoria', 
        'Provincia', 'Localidad', 'Nombre', 'Domicilio', 'Cod_Postal', 'Telefono', 'Mail', 'Web', 'Fuente']

    fecha = dt.datetime.today().strftime("%m/%d/%Y")
    
    df_cultural = pd.DataFrame()
    df_cultural = pd.concat([df_museos, df_cines, df_bibliotecas])
    df_cultural.insert(0, "Fecha", fecha)
    df_cultural = df_cultural.set_index('Fecha')

    #print(df_cultural.isnull().sum())
    df_cultural = df_cultural.fillna('NaN')

    logger.info(f'DataFrame Cultural was created!: \n\n{df_cultural}\n')

    return df_cultural


def create_df_registros(df_cultural):

    # Categoria
    df_categoria = df_cultural.groupby(['Categoria']).size().reset_index(name='Total')      
    logger.info(f"Cantidad de registros totales por 'Categoria': \n\n{df_categoria}\n")
    
    # Fuentes
    df_fuentes = df_cultural.groupby(['Fuente']).size().reset_index(name='Total')
    logger.info(f"Cantidad de registros totales por 'Fuente': \n\n{df_fuentes}\n")
    
    # Provincia y Categorias
    df_prov_categ = df_cultural.groupby(['Provincia', 'Categoria']).size().reset_index(name='Total')
    logger.info(f"Cantidad de registros totales por 'Provincia' y 'Categoria': \n\n{df_prov_categ}\n")
    
    fecha = dt.datetime.today().strftime("%m/%d/%Y")
    
    # Total Registros
    df_registros = pd.concat([df_categoria, df_fuentes, df_prov_categ], axis = 1)  
    df_registros.insert(0, "Fecha", fecha)
    df_registros = df_registros.set_index('Fecha')
    df_registros = df_registros.fillna('NaN')

    logger.info(f"Cantidad de registros totales: \n\n{df_registros}\n")

    return df_registros


def create_df_cines(df_cines):

    df_cines_prov = pd.DataFrame()
    df_cines_prov['Pantallas'] = df_cines.Pantallas.groupby(df_cines['Provincia']).sum().to_frame()
    df_cines_prov['Butacas'] = df_cines.Butacas.groupby(df_cines['Provincia']).sum().to_frame()
    df_cines_prov['Espacios INCAA'] = df_cines.espacio_INCAA.groupby(df_cines['Provincia']).count()

    fecha = dt.datetime.today().strftime("%m/%d/%Y")
    
    df_cines_prov.insert(3, "Fecha", fecha)
    #df_cines_prov = df_cines_prov.set_index('Fecha')
    df_cines_prov = df_cines_prov.fillna('NaN')
    
    logger.info(f"Informacion de 'Cines': \n\n{df_cines_prov}\n")

    return df_cines_prov
