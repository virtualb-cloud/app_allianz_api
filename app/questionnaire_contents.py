# Allianz v2.0

import numpy as np
import pandas as pd

class Contenter:

    def __init__(self) -> None:
            
        self.MIFID_2018_columns =[
            'ETA', 'SESSO_B', 'PROV_T', 'PROFESSIONE_S', 'TAE_T', 'CAP_S',
            'VAL_DOMANDA_S1_1', 'VAL_DOMANDA_S1_2', 'VAL_DOMANDA_S1_3',
            'VAL_DOMANDA_S1_4', 'VAL_DOMANDA_S2_5A_1', 'VAL_DOMANDA_S2_5A_2',
            'VAL_DOMANDA_S2_5A_3', 'VAL_DOMANDA_S2_5A_4', 'VAL_DOMANDA_S2_5A_5',
            'VAL_DOMANDA_S2_5A_6', 'VAL_DOMANDA_S2_5A_7', 'VAL_DOMANDA_S2_5A_8',
            'VAL_DOMANDA_S2_5A_9', 'VAL_DOMANDA_S2_5A_10', 'VAL_DOMANDA_S2_5A_11',
            'VAL_DOMANDA_S2_5A_12', 'VAL_DOMANDA_S2_5B_1', 'VAL_DOMANDA_S2_5B_2',
            'VAL_DOMANDA_S2_5B_3', 'VAL_DOMANDA_S2_5B_4', 'VAL_DOMANDA_S2_5B_5',
            'VAL_DOMANDA_S2_5B_6', 'VAL_DOMANDA_S2_5B_7', 'VAL_DOMANDA_S2_5B_8',
            'VAL_DOMANDA_S2_5B_9', 'VAL_DOMANDA_S2_5B_10', 'VAL_DOMANDA_S2_5B_11',
            'VAL_DOMANDA_S2_5B_12', 'VAL_DOMANDA_S2_6_1', 'VAL_DOMANDA_S2_6_2',
            'VAL_DOMANDA_S2_6_3', 'VAL_DOMANDA_S2_6_4',  'VAL_DOMANDA_S2_7', 
            'VAL_DOMANDA_S2_8', 'VAL_DOMANDA_S3_9', 'VAL_DOMANDA_S3_10',
            'VAL_DOMANDA_S3_11', 'VAL_DOMANDA_S3_12', 'VAL_DOMANDA_S4_13_1', 
            'VAL_DOMANDA_S4_13_2', 'VAL_DOMANDA_S4_13_3', 'VAL_DOMANDA_S4_14_1',
            'VAL_DOMANDA_S4_14_2', 'VAL_DOMANDA_S4_14_3',
            'VAL_DOMANDA_S4_14_4', 'VAL_DOMANDA_S4_15', 
        ]
        self.MIFID_2020_columns = [
            'NASCITA_FIGLIO_1_MU20', 
            'NASCITA_FIGLIO_2_MU20',
            'NASCITA_FIGLIO_3_MU20', 
            'NASCITA_FIGLIO_4_MU20',
            'NASCITA_FIGLIO_5_MU20', 
            'NASCITA_FIGLIO_6_MU20',
            'VAL_DOMANDA_S2_7_MU20',
            'VAL_DOMANDA_S2_8_MU20', 
            'VAL_DOMANDA_S2_9_MU20',
            'VAL_DOMANDA_S2_10_MU20'
            'VAL_DOMANDA_S4_13_1', 
            'VAL_DOMANDA_S4_13_2', 
            'VAL_DOMANDA_S4_13_3',
            'VAL_DOMANDA_S4_17_4_MU20', 
            'VAL_DOMANDA_S4_17_5_MU20', 
            'VAL_DOMANDA_S4_18_MU20'
        ]
        self.MIFID_2022_columns = [
            "VAL_DOMANDA_S5_21_MU22",
            "VAL_DOMANDA_S5_22_MU22",
            "VAL_DOMANDA_S5_23_MU22"
        ]


# 1) socio demographics:

    def v1_ordinal_age(self, external_df:pd.DataFrame):

        df = external_df

        ##### overall info
        column = "ETA"

        array = external_df[column]
        
        max = 120

        array = array / max

        return max, array

    def v1_categorical_gender(self, external_df:pd.DataFrame) -> pd.Series:

        ##### overall info
        column = "SESSO_B"
        
        gender_map = {
            'FEMMINILE': "f",
            'MASCHILE': "m"
        }
        array = external_df[column].map(gender_map).fillna("m")

        return array

    def v1_categorical_profession(self, external_df:pd.DataFrame) -> pd.Series:

        ##### overall info
        column = "PROFESSIONE_S"

        array = external_df[column]

        replacements = {
            '01 DIRIGENTE' : "DIRIGENTE",
            '02 IMPRENDITORE' : "IMPRENDITORE",
            '03 LAVORATORE AUTONOMO' : "LAVORATORE_AUTONOMO",
            '04 LIBERO PROFESSIONISTA' : "LIBERO_PROFESSIONISTA",
            '05 PENSIONATO' : "PENSIONATO",
            '06 LAV. DIPENDENTE A TEMPO DETERMINATO' : "DIPENDENTE_DETERMINATO",
            '07 LAV. DIPENDENTE A TEMPO INDETERMINATO' : "DIPENDENTE_INDETERMINATO",
            '08 NON OCCUPATO (STUDENTI/CASALINGHE)' : "NON_OCCUPATO"
        }
        profession_map = {
            'DIRIGENTE' : "lavoratore dipendente",
            'IMPRENDITORE' : "lavoratore indipendente",
            'LAVORATORE_AUTONOMO' : "lavoratore indipendente",
            'LIBERO_PROFESSIONISTA' : "lavoratore indipendente",
            'PENSIONATO' : "pensionato",
            'DIPENDENTE_DETERMINATO' : "lavoratore dipendente",
            'DIPENDENTE_INDETERMINATO' : "lavoratore dipendente",
            'NON_OCCUPATO' : "non occupato"
        }
        
        array = array.map(replacements).map(profession_map).fillna("non occupato")

        return array
    
    def v1_categorical_location_region(self, external_df:pd.DataFrame) -> pd.Series:

        ##### overall info
        column = "PROV_T"

        array = external_df[column]

        replacements = {
 
            'AO' : "aosta",

            'TO' : "piemonte", 
            'NO' : "piemonte", 
            'VB' : "piemonte", 
            'AL' : "piemonte", 
            'VC' : "piemonte", 
            'AT' : "piemonte",  
            'BI' : "piemonte",
            'CN' : "piemonte", 

            'LO' : "lombardia", 
            'CO' : "lombardia", 
            'MI' : "lombardia",
            'MN' : "lombardia",
            'VA' : "lombardia", 
            'MB' : "lombardia",
            'BS' : "lombardia", 
            'BG' : "lombardia", 
            'CR' : "lombardia", 
            'SO' : "lombardia", 
            'PV' : "lombardia", 

            'PN' : "friuli venezia giulia",
            'GO' : "friuli venezia giulia", 
            'TS' : "friuli venezia giulia", 
            'UD' : "friuli venezia giulia",

            'PI' : "toscana",
            'AR' : "toscana",
            'FI' : "toscana", 
            'SI' : "toscana",
            'LU' : "toscana", 
            'PO' : "toscana", 
            'PT' : "toscana",   
            'MS' : "toscana",
            'GR' : "toscana",     
            'LI' : "toscana", 

            'SV' : "liguria",
            'GE' : "liguria", 
            'IM' : "liguria", 
            'SP' : "liguria", 

            'TV' : "veneto", 
            'VE' : "veneto", 
            'VI' : "veneto", 
            'VR' : "veneto", 
            'PD' : "veneto", 
            'BL' : "veneto",      
            'RI' : "veneto", 
            'RO' : "veneto",

            'TR' : "umbria", 
            'PG' : "umbria",
        
            'TE' : "abruzzo",
            'AQ' : "abruzzo", 
            'PE' : "abruzzo", 
            'CH' : "abruzzo",

            'TN' : "trentino alto adige",
            'BZ' : "trentino alto adige",  

            'RN' : "emilia romagna", 
            'RE' : "emilia romagna", 
            'PR' : "emilia romagna", 
            'RA' : "emilia romagna", 
            'BO' : "emilia romagna",
            'FE' : "emilia romagna",
            'PC' : "emilia romagna",
            'MO' : "emilia romagna", 
            'FC' : "emilia romagna", 

            'FR' : "lazio", 
            'VT' : "lazio", 
            'LT' : "lazio",
            'RM' : "lazio", 

            'AN' : "marche",
            'FM' : "marche",  
            'MC' : "marche",
            'PU' : "marche", 
            'AP' : "marche",
            'PS' : "marche",   
        
            'CE' : "campania",
            'SA' : "campania", 
            'AV' : "campania",     
            'BN' : "campania", 

            'IS' : "molise", 
            'CB' : "molise",

            'PZ' : "basilicata",
            'MT' : "basilicata", 

            'CZ' : "calabria",
            'CS' : "calabria",
            'KR' : "calabria",
            'VV' : "calabria", 
            'RC' : "calabria", 

            'BA' : "puglia",
            'FG' : "puglia", 
            'BT' : "puglia", 
            'TA' : "puglia",
            'LE' : "puglia",
            'BR' : "puglia",

            'PA' : "sicilia",
            'CT' : "sicilia", 
            'ME' : "sicilia",
            'RG' : "sicilia", 
            'CL' : "sicilia",
            'EN' : "sicilia",
            'SR' : "sicilia", 
            'TP' : "sicilia", 
            'AG' : "sicilia", 

            'CA' : "sardinia",
            'SS' : "sardinia",
            'SU' : "sardinia",
            'OT' : "sardinia",
            'NU' : "sardinia",
            'OR' : "sardinia",
            'OG' : "sardinia", 
            'CI' : "sardinia", 

            'EE' : "lombardia", 
            'ZH' : "lombardia", 
            'UK' : "lombardia"
        }

        array = array.map(replacements)

        return array.fillna("lombardia")

    def v1_categorical_education(self, external_df:pd.DataFrame) -> pd.Series:

        ##### overall info
        column = "VAL_DOMANDA_S1_4"

        array = external_df[column]

        values = {
            "1.4.1" : "A. Diploma",
            "1.4.2" : "B. Laurea/Specializzazione post laurea",
            "1.4.3" : "C. Diploma/Laurea/Specializzazione post laurea in discipline economico-finanziarie",
            "1.4.4" : "D. Altro"
        }

        replacements = {
            '1.4.1': "diploma",
            '1.4.2': "laurea",
            '1.4.3': "laura_economica",
            '1.4.4': "elementari"
        }

        education_map = {
            'diploma': "scuola secondaria di II grado",
            'laurea': "istruzione superiore università",
            'laura_economica': "istruzione superiore università",
            'elementari': "scuola primaria"
        }

        array = array.map(replacements).map(education_map).fillna("scuola primaria")

        return array


# 2) family status:

    def v1_categorical_houses_count(self, external_df:pd.DataFrame) -> pd.Series:

        column = "VAL_DOMANDA_S3_11"

        array = external_df[column]

        values = {
            "3.11.1" : "A. Non possiedo immobili",
            "3.11.2" : "B. Sì, solo la prima casa",
            "3.11.3" : "C. Sì, la prima casa e ulteriori proprietà immobiliari",
            "3.11.11" : "A. Non possiedo immobili",
            "3.11.12" : "B. Sì, solo un immobile",
            "3.11.13" : "C. Sì, più di un immobile"
        }

        replacements = {
            '3.11.1': "zero_houses",
            '3.11.2': "one_house",
            '3.11.3': "more_houses",
            '3.11.11': "zero_houses",
            '3.11.12': "one_house",
            '3.11.13': "more_houses",
        }

        array = array.map(replacements).fillna("zero_houses")

        return array
    
    def v1_categorical_marital_status(self, external_df:pd.DataFrame) -> pd.Series:

        ##### overall info
        column = "VAL_DOMANDA_S1_1"

        array = external_df[column]

        values = {
            "1.1.1" : "A. Nubile/celibe",
            "1.1.2" : "B. Coniugato/a o convivente",
            "1.1.3" : "C. Separato/a o divorziato/a",
            "1.1.4" : "D. Vedovo/a"
        }

        replacements = {
            '1.1.1': "Nubile",
            '1.1.2': "Coniugato",
            '1.1.3': "Separato",
            '1.1.4': "Vedovo"
        }   

        array = array.map(replacements).fillna("Nubile")
        
        return array

    def v1_ordinal_childrens_count(self, external_df:pd.DataFrame):

        ##### overall info
        column = "VAL_DOMANDA_S1_2"

        array = external_df[column]

        values = {
            "1.2.1" : "A. No",
            "1.2.2" : "B. Sì, uno",
            "1.2.3" : "C. Sì, due",
            "1.2.4" : "D. Sì, più di due"
        }

        replacements = {
            '1.2.1': 1,
            '1.2.2': 2,
            '1.2.3': 3,
            '1.2.4': 4
        } 

        max = 4

        array = array.map(replacements).fillna(0)

        array = array / max

        return max, array

    def v1_ordinal_dependents_count(self, external_df:pd.DataFrame):

        ##### overall info
        column = "VAL_DOMANDA_S1_3"

        array = external_df[column]

        values = {
            "1.3.1" : "A. Sì, una persona",
            "1.3.2" : "B. Sì, due persone",
            "1.3.3" : "C. Sì, tre persone o più",
            "1.3.4" : "D. No, nessuna"
        }

        replacements = {
            '1.3.1': 1,
            '1.3.2': 2,
            '1.3.3': 3,
            '1.3.4': 0
        }   
        
        max = 3

        array = array.map(replacements).fillna(0)

        array = array / max
        
        return max, array
  
    def v2_ordinal_average_childrens_age(self) -> dict:
        
        column = "NASCITA_FIGLIO_1_MU20"

        destination_variables = ["family_members_number"]
        nome_entità = "DECOD_PRF_NASCITA_FIGLIO_NUMERO_FIGLI"
        note = "anno di nascita del figlio se indicato"

        return note


# 3) financial status:

    def v1_ordinal_yearly_ctv(self, external_df:pd.DataFrame):
        
        ##### overall info
        column = "VAL_DOMANDA_S2_7"

        array = external_df[column]

        values = {
            "2.7.1" : "A. Minore di 50.000 Euro in 3 anni",
            "2.7.2" : "B. Compreso tra 50.000 Euro e 150.000 Euro in 3 anni",
            "2.7.3" : "C. Maggiore di 150.000 Euro in 3 anni",
            "2.7.4" : "D. Nessuna operazione in 3 anni"
        }
        
        # considering mid-bins
        bin_size = 3
        max = 300000
        years = 3
        bins = [round(25000/years), round(100000/years), round(200000/years)]

        replacements = {
            '2.7.1': bins[0],
            '2.7.2': bins[1],
            '2.7.3': bins[2],
            '2.7.4': 0
        }  

        array = array.map(replacements).fillna(0)
        
        array = array / max
        
        return max, array

    def v1_ordinal_yearly_income(self, external_df:pd.DataFrame):
        
        ##### overall info
        column = "VAL_DOMANDA_S3_9"
        
        array = external_df[column]

        values = {
            "3.9.1" : "A. Fino a 100.000 Euro",
            "3.9.2" : "B. Oltre 100.000 Euro",
            "3.9.11" : "A. Fino a 50.000 Euro",
            "3.9.12" : "B. da 50.000 a 100.000 Euro",
            "3.9.13" : "C. Da 100.000 a 250.000 Euro",
            "3.9.14" : "D. Oltre 250.000 Euro"
        }

        # considering mid-bins
        bin_size = 2
        max_1 = 200000
        bins_1 = [50000, 150000]

        # considering mid-bins
        bin_size = 4
        max_2 = 450000
        bins_2 = [25000, 75000, 175000, 350000]

        # bin adjustment
        bins_1 = bins_1 * round(max_2/max_1)
        
        replacements = {
            '3.9.1': bins_1[0],
            '3.9.2': bins_1[1],
            '3.9.11': bins_2[0],
            '3.9.12': bins_2[1],
            '3.9.13': bins_2[2],
            '3.9.14': bins_2[3],
        }

        array = array.map(replacements).fillna(0)
        
        array = array / max_2

        return max_2, array

    def v1_ordinal_out_aum(self, external_df:pd.DataFrame):

        ##### overall info
        column = "VAL_DOMANDA_S3_10"

        array = external_df[column]

        values = {
            "3.10.1" : "A. Non detengo nulla presso altri intermediari",
            "3.10.2" : "B. Fino a 10.000 Euro",
            "3.10.3" : "C. Da 10.000 Euro a 100.000 Euro",
            "3.10.4" : "D. Da 100.000 Euro a 250.000 Euro",
            "3.10.5" : "E. Da 250.000 Euro a 1.000.000 Euro",
            "3.10.6" : "F. Oltre 1.000.000 Euro",
            "3.10.11" : "B. Fino a 10.000 Euro",
            "3.10.12" : "C. Da 10.000 Euro a 100.000 Euro",
            "3.10.13" : "D. Da 100.000 Euro a 250.000 Euro",
            "3.10.14" : "E. Da 250.000 Euro a 1.000.000 Euro",
            "3.10.15" : "F. Oltre 1.000.000 Euro"
        }
        
        # considering mid-bins
        bin_size = 5
        max = 2000000
        bins = [5000, 55000, 175000, 725000, 1500000]

        replacements = {
            '3.10.1': 0,
            '3.10.2': bins[0],
            '3.10.3': bins[1],
            '3.10.4': bins[2],
            '3.10.5': bins[3],
            '3.10.6': bins[4],
            '3.10.11': bins[0],
            '3.10.12': bins[1],
            '3.10.13': bins[2],
            '3.10.14': bins[3],
            '3.10.15': bins[4],
        }

        array = array.map(replacements).fillna(0)
        
        array = array / max

        return max, array

    def v1_ordinal_yearly_liabilities(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S3_12"
        
        array = external_df[column]

        values = {
            "3.12.1" : "A. Nulla, non ho impegni finanziari regolari",
            "3.12.2" : "B. Bassa (meno del 10%)",
            "3.12.3" : "C. Media (tra il 10% e il 40%)",
            "3.12.4" : "D. Alta (oltre il 40%)"
        }


        # considering mid-bins
        bin_size = 3
        max_index = 0.9
        bins = [0.05, 0.25, 0.55]

        replacements = {
            "3.12.1" : 0,
            "3.12.2" : bins[0],
            "3.12.3" : bins[1],
            "3.12.4": bins[2]
        }  

        array = array.map(replacements).fillna(0)

        max_income, array2 = self.v1_ordinal_yearly_income(external_df)

        # max

        max = max_index * max_income

        return max, array


# 4) financial culture:

    def v1_ordinal_financial_knowledge(self, external_df:pd.DataFrame):

        ##### overall info
        columns = [
            "VAL_DOMANDA_S2_5A_1", "VAL_DOMANDA_S2_5A_2",
            "VAL_DOMANDA_S2_5A_3", "VAL_DOMANDA_S2_5A_4",
            "VAL_DOMANDA_S2_5A_5", "VAL_DOMANDA_S2_5A_6",
            "VAL_DOMANDA_S2_5A_7", "VAL_DOMANDA_S2_5A_8",
            "VAL_DOMANDA_S2_5A_9", "VAL_DOMANDA_S2_5A_10",
            "VAL_DOMANDA_S2_5A_11", "VAL_DOMANDA_S2_5A_12"
            ]

        df = external_df[columns]

        values = {
            "S" : "SI",
            "N" : "NO"
        }

        ##### map

        # considering boolean
        bin_size = 2
        bins = [0, 1]
 
        replacements = {
            'S': bins[1],
            'N': bins[0]
        }

        for column in columns:
            df[column] = df[column].map(replacements).fillna(0)
        
        max = len(columns)

        array = df[columns].sum(numeric_only=True, axis=1)/max
        
        return max, array

    def v1_ordinal_financial_experience(self, external_df:pd.DataFrame):

        ##### overall info
        columns = [
            "VAL_DOMANDA_S2_5B_1", "VAL_DOMANDA_S2_5B_2",
            "VAL_DOMANDA_S2_5B_3", "VAL_DOMANDA_S2_5B_4",
            "VAL_DOMANDA_S2_5B_5", "VAL_DOMANDA_S2_5B_6",
            "VAL_DOMANDA_S2_5B_7", "VAL_DOMANDA_S2_5B_8",
            "VAL_DOMANDA_S2_5B_9", "VAL_DOMANDA_S2_5B_10",
            "VAL_DOMANDA_S2_5B_11", "VAL_DOMANDA_S2_5B_12"
            ]

        df = external_df[columns]

        values = {
            "S" : "SI",
            "N" : "NO"
        }


        ##### map

        # considering boolean
        bin_size = 2
        bins = [0, 1]
 
        replacements = {
            'S': bins[1],
            'N': bins[0]
        }

        for column in columns:
            df[column] = df[column].map(replacements).fillna(0)

        max = len(columns)

        array = df[columns].sum(numeric_only=True, axis=1)/max

        return max, array

    def v1_ordinal_correct_financial_answers(self, external_df:pd.DataFrame):

        ##### overall info
        columns = [
            "VAL_DOMANDA_S2_6_1", "VAL_DOMANDA_S2_6_2",
            "VAL_DOMANDA_S2_6_3", "VAL_DOMANDA_S2_6_4"
            ]

        df = external_df[columns]

        values = {
            "A" : "ALTO",
            "B" : "BASSO"
        }
        
        true_answers = {
            "VAL_DOMANDA_S2_6_1" : "1",
            "VAL_DOMANDA_S2_6_2" : "0",
            "VAL_DOMANDA_S2_6_3" : "0",
            "VAL_DOMANDA_S2_6_4" : "1"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        bins = [0, 1]
 
        replacements = {
            'A': bins[1],
            'B': bins[0]
        }

        for column in columns:
            df[column] = df[column].map(replacements).fillna(0)

        ##### check
        df = df.assign(rating=0)
        for idx, row in df.iterrows():
            
            rating = 0
            if df.loc[idx, columns[0]] == true_answers[columns[0]] : 
                rating += 1
            
            if df.loc[idx, columns[1]] == true_answers[columns[1]] : 
                rating += 1

            if df.loc[idx, columns[2]] == true_answers[columns[2]] : 
                rating += 1

            if df.loc[idx, columns[3]] == true_answers[columns[3]] : 
                rating += 1
            
            df.loc[idx, "rating"] = rating

        max = len(columns)

        array = df["rating"] / max

        return max, array

    def v1_ordinal_yearly_trading_frequency(self, external_df:pd.DataFrame):

        ##### overall info
        column = "VAL_DOMANDA_S2_8"

        array = external_df[column]

        values = {
            "2.8.1" : "A. Minore di 6 in tre anni",
            "2.8.2" : "B. Compreso tra 6 e 15 in tre anni",
            "2.8.3" : "C. Maggiore di 15 in tre anni",
            "2.8.4" : "D. Nessuna operazione in tre anni"
        }

        # considering mid-bins 
        bin_size = 3
        max = 30
        years = 3
        bins = [round(3/years), round(10/years), round(22.5/years)]
 
        replacements = {
            '2.8.1': bins[0],
            '2.8.2': bins[1],
            '2.8.3': bins[2],
            '2.8.4': 0
        }

        max = max / years
        array = array.map(replacements).fillna(0)
        array = array / max

        return max, array
    
    def v1_ordinal_subjective_time_horizon(self, external_df:pd.DataFrame):

        ##### overall info
        note = "percentuale dichiarata per orizzonte fino a 1 anno, 3 anni, 5 anni, e più di 5"

        columns = [
            'VAL_DOMANDA_S4_14_1', 'VAL_DOMANDA_S4_14_2', 
            'VAL_DOMANDA_S4_14_3', 'VAL_DOMANDA_S4_14_4'
        ]

        ##### initiate
        df = external_df[columns]

        ##### map

        # considering max-bins
        bin_size = 4
        max_horizon = 10
        bins = [1, 3, 5, 10]

        ##### max 
        values = bins
        
        df = df.assign(result=0)
        for idx, row in df.iterrows():
            
            weights = []

            for column in columns:
                
                weights.append(df.loc[idx, column])

            df.loc[idx, "result"] = np.dot(weights, values)

        array = df["result"].fillna(0) / max_horizon

        return max_horizon, array

    def v1_ordinal_subjective_risk(self, external_df:pd.DataFrame):

        column = 'VAL_DOMANDA_S4_15'

        array = external_df[column]
        
        values = {
            "4.15.1" : "A. Disinvestirei immediatamente perché non sarei disposto ad accettare ulteriori perdite",
            "4.15.2" : "B. Manterrei l'investimento in attesa che recuperi il valore prima di vendere",
            "4.15.3" : "C. Manterrei l'investimento in modo da ottenere un rendimento positivo di lungo periodo",
            "4.15.4": "D. Manterrei l'investimento e incrementerei parzialmente la posizione per sfruttare la discesa del mercato"
        }

        # considering mid-bins
        bin_size = 4
        max = 1
        bins = [0.125, 0.375, 0.625, 0.875]
        
        replacements = {
            "4.15.1" : bins[0],
            "4.15.2" : bins[1],
            "4.15.3" : bins[2],
            "4.15.4": bins[3]
        }  

        array = array.map(replacements).fillna(0)

        return max, array

    def v1_bool_sophisticated_instrument_presence(self, external_df:pd.DataFrame):

        # map
        columns = [
            "VAL_DOMANDA_S2_5A_10", "VAL_DOMANDA_S2_5A_11", "VAL_DOMANDA_S2_5A_12"
            ]

        df = external_df[columns]
        
        values = {
            "S" : "SI",
            "N" : "NO"
        }

        ##### map

        # considering boolean
        bin_size = 2
        max = 1
        bins = [0, 1]
 
        replacements = {
            'S': bins[1],
            'N': bins[0]
        }

        for column in columns:
            df[column] = df[column].map(replacements).fillna(0)

        ##### check
        df = df.assign(result=0)
        for idx, row in df.iterrows():
            for column in columns:
                if df.loc[idx, column] == 1:
                    df.loc[idx, "result"] = 1
                    break

        return df["result"]

    def v1_ordinal_objective_time_horizon(self, external_df:pd.DataFrame) -> pd.DataFrame:
        
        df = pd.DataFrame()
        
        max, df["liquidity"] = self.v1_ordinal_subjective_liquidity_investment_need(external_df)
        max, df["capital"] = self.v1_ordinal_subjective_capital_accumulation_investment_need(external_df)
        max, df["retirement"] = self.v1_ordinal_subjective_retirement_investment_need(external_df)

        max_time = 10
        bins = [1, 3, 5, 10]
        
        df = df.assign(result=0)
        for idx, row in df.iterrows():

            if df.loc[idx, "capital"] > 0.5:
                
                if df.loc[idx, "retirement"] > 0.5: horizon = bins[3]
                elif df.loc[idx, "retirement"] > 0: horizon = bins[2]
                elif df.loc[idx, "retirement"] == 0: horizon = bins[1]

            elif df.loc[idx, "capital"] > 0:

                if df.loc[idx, "retirement"] > 0.5: horizon = bins[2]
                elif df.loc[idx, "retirement"] > 0: horizon = bins[1]
                elif df.loc[idx, "retirement"] == 0: horizon = bins[1]

            elif df.loc[idx, "capital"] == 0:

                if df.loc[idx, "retirement"] > 0.5: horizon = bins[2]
                elif df.loc[idx, "retirement"] > 0: horizon = bins[1]
                elif df.loc[idx, "retirement"] == 0: 
        
                    if df.loc[idx, "liquidity"] > 0.5: horizon = bins[2]
                    elif df.loc[idx, "liquidity"] > 0: horizon = bins[1]
                    elif df.loc[idx, "liquidity"] > 0: horizon = 0

            df.loc[idx, "result"] = horizon

        array = df["result"].fillna(0) / max_time

        return max_time, array

    def v2_ordinal_correct_financial_answers(self, external_df:pd.DataFrame):


        ##### overall info
        columns = [
            'VAL_DOMANDA_S2_7_MU20',
            'VAL_DOMANDA_S2_8_MU20', 
            'VAL_DOMANDA_S2_9_MU20',
            'VAL_DOMANDA_S2_10_MU20'
        ]

        df = external_df[columns]

        replacements = {
            "2.9.1" : "A",
            "2.9.2" : "B",
            "2.9.3" : "C",
            "2.10.1" : "A",
            "2.10.2" : "B",
            "2.10.3" : "C",
            "2.11.1" : "A",
            "2.11.2" : "B",
            "2.11.3" : "C",
            "2.12.1" : "A",
            "2.12.2" : "B",
            "2.12.3" : "C"
        }

        true_answers = {
            "VAL_DOMANDA_S2_7_MU20" : "A",
            "VAL_DOMANDA_S2_8_MU20" : "B",
            "VAL_DOMANDA_S2_9_MU20" : "B",
            "VAL_DOMANDA_S2_10_MU20" : "C"
        }
        
        ##### map

        for column in columns:
            df[column] = df[column].map(replacements).fillna(0)

        ##### check
        df = df.assign(rating=0)
        for idx, row in df.iterrows():
            
            rating = 0
            if df.loc[idx, columns[0]] == true_answers[columns[0]] : 
                rating += 1
            
            if df.loc[idx, columns[1]] == true_answers[columns[1]] : 
                rating += 1

            if df.loc[idx, columns[2]] == true_answers[columns[2]] : 
                rating += 1

            if df.loc[idx, columns[3]] == true_answers[columns[3]] : 
                rating += 1
            
            df.loc[idx, "rating"] = rating

        max = len(columns)

        array = df["rating"]/max
        
        return max, array


# 5) financial needs:
        
    # 5_1) investments:

    def v1_ordinal_subjective_liquidity_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_13_1"

        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)

        return max, array

    def v1_ordinal_subjective_capital_accumulation_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_13_2"
        
        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)
        
        return max, array

    def v1_ordinal_subjective_income_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_13_2"

        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)
        
        return max, array

    def v1_ordinal_subjective_retirement_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_13_3"

        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)
        
        return max, array

    def v2_ordinal_subjective_liquidity_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_13_1"
        
        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)

        array = array / max
        
        return max, array

    def v2_ordinal_subjective_capital_accumulation_investment_need(self, external_df:pd.DataFrame):

        columns = ["VAL_DOMANDA_S4_13_2", "VAL_DOMANDA_S4_17_5_MU20"]

        df = external_df[columns]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        for column in columns:
            df[column].map(replacements).fillna(0)
        

        array = df[columns].sum(numeric_only=True, axis=1)

        array = array / max

        return max, array

    def v2_ordinal_subjective_income_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_13_3"

        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)

        array = array / max
        
        return max, array

    def v2_ordinal_subjective_capital_protection_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_17_4_MU20"

        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)

        array = array / max
        
        return max, array

    def v2_ordinal_subjective_retirement_investment_need(self, external_df:pd.DataFrame):

        column = "VAL_DOMANDA_S4_17_5_MU20"

        array = external_df[column]

        values = {
            "P" : "Principale",
            "I" : "Secondaria",
            "N" : "Non scelta/vuota"
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        max = 1
        bins = [0.25, 0.75]
        
        replacements = {
            'I': bins[0],
            'P': bins[1],
            'N': 0,
        }

        array = array.map(replacements).fillna(0)

        array = array / max
        
        return max, array


    # 6) personal culture:

    def v3_bool_declared_esg_propensity(self, external_df:pd.DataFrame) -> pd.DataFrame:


        column = "VAL_DOMANDA_S5_21_MU22"

        array = external_df[column]

        values = {
            "A" : "A. Si, sono interessato",
            "B" : "B. No, non sono interessato"
        }

        # map
        # keys to be modified
        mappa = {
            "A" : 1,
            "B" : 0
        }

        array = array.map(mappa).fillna(0)
        
        return array

    def v3_nominal_declared_esg_propensity(self, external_df:pd.DataFrame) -> pd.DataFrame:

        column = "VAL_DOMANDA_S5_22_MU22"

        array = external_df[column]

        values = {
            "A" : "A. In misura almeno pari al 10%",
            "B" : "B. In misura almeno pari al 20%",
            "C" : "C. In misura almeno pari al 40%"
        }

        ##### map
        
        # considering mid-bins
        bin_size = 3
        bins = [0.167, 0.500, 0.833]

        # keys to be modified
        replacements = {
            "A" : bins[0],
            "B" : bins[1],
            "C" : bins[2]
        }

        max = 1

        array = array.map(replacements).fillna(0)
        
        return max, array

    def v3_bool_evironment_propensity_index(self, external_df:pd.DataFrame) -> pd.DataFrame:
        
        # first look
        column = "VAL_DOMANDA_S5_23_MU22"

        array = external_df[column]

        # map
        values = {
            "A" : "A. Le tematiche di sostenibilità ambientale (riconducibili, per esempio, al contrasto dei cambiamenti climatici, della perdita di biodiversità, del consumo eccessivo di risorse a livello mondiale, della scarsità alimentare, della riduzione dello strato di ozono, dell’a- cidificazione degli oceani, del deterioramento del sistema di acqua dolce e dei cambia- menti di destinazione dei terreni)",
            "B" : "B. Le tematiche di sostenibilità ed equità sociale (riconducibili, per esempio, alla lotta contro la disuguaglianza, alla promozione della coesione sociale, dell’integrazione sociale e delle relazioni industriali, o all’investimento in capitale umano o in comunità economicamente o socialmente svantaggiate)",
            "C" : "C. Non ho una specifica preferenza"
        }

        # keys to be modified
        replacements = {
            "A" : "A",
            "B" : "B",
            "C" : "C"
        }

        array = array.map(replacements).fillna(0)

        # check

        for idx, val in array.items():
            
            if val == "A":
                array.loc[idx] = 1

            else:
                array.loc[idx] = 0

        return array

    def v3_bool_social_propensity_index(self, external_df:pd.DataFrame) -> pd.DataFrame:
        
        # first look
        column = "VAL_DOMANDA_S5_23_MU22"

        array = external_df[column]

        # map 
        values = {
            "A" : "A. Le tematiche di sostenibilità ambientale (riconducibili, per esempio, al contrasto dei cambiamenti climatici, della perdita di biodiversità, del consumo eccessivo di risorse a livello mondiale, della scarsità alimentare, della riduzione dello strato di ozono, dell’a- cidificazione degli oceani, del deterioramento del sistema di acqua dolce e dei cambia- menti di destinazione dei terreni)",
            "B" : "B. Le tematiche di sostenibilità ed equità sociale (riconducibili, per esempio, alla lotta contro la disuguaglianza, alla promozione della coesione sociale, dell’integrazione sociale e delle relazioni industriali, o all’investimento in capitale umano o in comunità economicamente o socialmente svantaggiate)",
            "C" : "C. Non ho una specifica preferenza"
        }

        # keys to be modified
        replacements = {
            "A" : "A",
            "B" : "B",
            "C" : "C"
        }
        array = array.map(replacements).fillna(0)

        # check

        for idx, val in array.items():

            if val == "B":
                array.loc[idx] = 1

            else:
                array.loc[idx] = 0

        return array

    def v3_bool_governance_propensity_index(self, external_df:pd.DataFrame) -> pd.DataFrame:
        
        # first look
        column = "VAL_DOMANDA_S5_23_MU22"

        array = external_df[column]

        # map
        values = {
            "A" : "A. Le tematiche di sostenibilità ambientale (riconducibili, per esempio, al contrasto dei cambiamenti climatici, della perdita di biodiversità, del consumo eccessivo di risorse a livello mondiale, della scarsità alimentare, della riduzione dello strato di ozono, dell’a- cidificazione degli oceani, del deterioramento del sistema di acqua dolce e dei cambia- menti di destinazione dei terreni)",
            "B" : "B. Le tematiche di sostenibilità ed equità sociale (riconducibili, per esempio, alla lotta contro la disuguaglianza, alla promozione della coesione sociale, dell’integrazione sociale e delle relazioni industriali, o all’investimento in capitale umano o in comunità economicamente o socialmente svantaggiate)",
            "C" : "C. Non ho una specifica preferenza"
        }

        # keys to be modified
        replacements = {
            "A" : "A",
            "B" : "B",
            "C" : "C"
        }
        array = array.map(replacements).fillna(0)

        # check

        for idx, val in array.items():

            if val == "B":
                array.loc[idx] = 1

            else:
                array.loc[idx] = 0

        return array


        