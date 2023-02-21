# Allianz v2.0

import numpy as np

class Contenter:

    def __init__(self) -> None:
            
        pass


# 1) socio demographics:

    def v1_ordinal_age(self, person:dict):

        ##### overall info
        column = "ETA"

        array = person[column]
        
        max = 120

        array = array / max

        return max, array

    def v1_categorical_gender(self, person:dict):

        ##### overall info
        column = "SESSO_B"
        
        replacements = {
            'FEMMINILE': "f",
            'MASCHILE': "m",
            "" : "f"
        }
        array = replacements[person[column]]

        return array

    def v1_categorical_profession(self, person:dict):

        ##### overall info
        column = "PROFESSIONE_S"

        replacements = {
            '01 DIRIGENTE' : "lavoratore dipendente",
            '02 IMPRENDITORE' : "lavoratore indipendente",
            '03 LAVORATORE AUTONOMO' : "lavoratore indipendente",
            '04 LIBERO PROFESSIONISTA' : "lavoratore indipendente",
            '05 PENSIONATO' : "pensionato",
            '06 LAV. DIPENDENTE A TEMPO DETERMINATO' : "lavoratore dipendente",
            '07 LAV. DIPENDENTE A TEMPO INDETERMINATO' : "lavoratore dipendente",
            '08 NON OCCUPATO (STUDENTI/CASALINGHE)' : "non occupato",
            "" : "non occupato"
        }

        array = replacements[person[column]]

        return array
    
    def v1_categorical_location_region(self, person:dict):

        ##### overall info
        column = "PROV_T"

        replacements = {
 
            'AO' : "valle daosta",

            'TO' : "piemonte", 
            'NO' : "piemonte", 
            'VB' : "piemonte", 
            'AL' : "piemonte", 
            'VC' : "piemonte", 
            'AT' : "piemonte",  
            'BI' : "piemonte",
            'CN' : "piemonte", 

            'LC' : "lombardia",
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

            'CA' : "sardegna",
            'SS' : "sardegna",
            'SU' : "sardegna",
            'OT' : "sardegna",
            'NU' : "sardegna",
            'OR' : "sardegna",
            'OG' : "sardegna", 
            'CI' : "sardegna", 

            'EE' : "lombardia", 
            'ZH' : "lombardia", 
            'UK' : "lombardia"
        }

        if person[column] in replacements.keys():
            array = replacements[person[column]]
        else:
            array = "lombardia"
            
        return array

    def v1_categorical_education(self, person:dict):

        ##### overall info
        column = "VAL_DOMANDA_S1_4"

        values = {
            "1.4.1" : "A. Diploma",
            "1.4.2" : "B. Laurea/Specializzazione post laurea",
            "1.4.3" : "C. Diploma/Laurea/Specializzazione post laurea in discipline economico-finanziarie",
            "1.4.4" : "D. Altro"
        }

        replacements = {
            '1.4.1': "scuola secondaria di II grado",
            '1.4.2': "istruzione superiore università",
            '1.4.3': "master di II livello e PHD",
            '1.4.4': "scuola primaria",
            '' : "scuola primaria"
        }

        array = replacements[person[column]]

        return array


# 2) family status:

    def v1_categorical_houses_count(self, person:dict):

        column = "VAL_DOMANDA_S3_11"

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
            '3.11.13': "more_houses"
        }

        array = replacements[person[column]]

        return array
    
    def v1_categorical_marital_status(self, person:dict):

        ##### overall info
        column = "VAL_DOMANDA_S1_1"

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

        array = replacements[person[column]]
        
        return array

    def v1_ordinal_childrens_count(self, person:dict):

        ##### overall info
        column = "VAL_DOMANDA_S1_2"

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

        array = replacements[person[column]]

        array = array / max

        return max, array

    def v1_ordinal_dependents_count(self, person:dict):

        ##### overall info
        column = "VAL_DOMANDA_S1_3"

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

        array = replacements[person[column]]

        array = array / max
        
        return max, array
  
    def v2_ordinal_average_childrens_age(self) -> dict:
        
        column = "NASCITA_FIGLIO_1_MU20"

        destination_variables = ["family_members_number"]
        nome_entità = "DECOD_PRF_NASCITA_FIGLIO_NUMERO_FIGLI"
        note = "anno di nascita del figlio se indicato"

        return note


# 3) financial status:

    def v1_ordinal_yearly_ctv(self, person:dict):
        
        ##### overall info
        column = "VAL_DOMANDA_S2_7"

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

        array = replacements[person[column]]
        
        array = array / max
        
        return max, array

    def v1_ordinal_yearly_income(self, person:dict):
        
        ##### overall info
        column = "VAL_DOMANDA_S3_9"

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

        array = replacements[person[column]]
        
        array = array / max_2

        return max_2, array

    def v1_ordinal_out_aum(self, person:dict):

        ##### overall info
        column = "VAL_DOMANDA_S3_10"

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

        array = replacements[person[column]]
        
        array = array / max

        return max, array

    def v1_ordinal_yearly_liabilities(self, person:dict):

        column = "VAL_DOMANDA_S3_12"
        
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

        array = replacements[person[column]]

        max_income, array2 = self.v1_ordinal_yearly_income(person=person)

        # max

        max = max_index * max_income

        return max, array


# 4) financial culture:

    def v1_ordinal_financial_knowledge(self, person:dict):

        ##### overall info
        columns = [
            "VAL_DOMANDA_S2_5A_1", "VAL_DOMANDA_S2_5A_2",
            "VAL_DOMANDA_S2_5A_3", "VAL_DOMANDA_S2_5A_4",
            "VAL_DOMANDA_S2_5A_5", "VAL_DOMANDA_S2_5A_6",
            "VAL_DOMANDA_S2_5A_7", "VAL_DOMANDA_S2_5A_8",
            "VAL_DOMANDA_S2_5A_9", "VAL_DOMANDA_S2_5A_10",
            "VAL_DOMANDA_S2_5A_11", "VAL_DOMANDA_S2_5A_12"
            ]

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

        sum = 0
        for column in columns:
            sum += replacements[person[column]]
        
        max = len(columns)

        array = sum / max
        
        return max, array

    def v1_ordinal_financial_experience(self, person:dict):

        ##### overall info
        columns = [
            "VAL_DOMANDA_S2_5B_1", "VAL_DOMANDA_S2_5B_2",
            "VAL_DOMANDA_S2_5B_3", "VAL_DOMANDA_S2_5B_4",
            "VAL_DOMANDA_S2_5B_5", "VAL_DOMANDA_S2_5B_6",
            "VAL_DOMANDA_S2_5B_7", "VAL_DOMANDA_S2_5B_8",
            "VAL_DOMANDA_S2_5B_9", "VAL_DOMANDA_S2_5B_10",
            "VAL_DOMANDA_S2_5B_11", "VAL_DOMANDA_S2_5B_12"
            ]

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

        sum = 0
        for column in columns:
            sum += replacements[person[column]]
        
        max = len(columns)

        array = sum / max
        
        return max, array

    def v1_ordinal_correct_financial_answers(self, person:dict):

        ##### overall info
        columns = [
            "VAL_DOMANDA_S2_6_1", "VAL_DOMANDA_S2_6_2",
            "VAL_DOMANDA_S2_6_3", "VAL_DOMANDA_S2_6_4"
            ]

        values = {
            "A" : "ALTO",
            "B" : "BASSO"
        }
        
        true_answers = {
            "VAL_DOMANDA_S2_6_1" : 1,
            "VAL_DOMANDA_S2_6_2" : 0,
            "VAL_DOMANDA_S2_6_3" : 0,
            "VAL_DOMANDA_S2_6_4" : 1
        }

        ##### map

        # considering mid-bins
        bin_size = 2
        bins = [0, 1]
 
        replacements = {
            'A': bins[1],
            'B': bins[0]
        }

        rating = 0
        for que, ans in true_answers.items():
            
            if replacements[person[que]] == ans:
                rating += 1

        max = len(true_answers.keys())

        array = rating / max

        return max, array

    def v1_ordinal_yearly_trading_frequency(self, person:dict):

        ##### overall info
        column = "VAL_DOMANDA_S2_8"

        values = {
            "2.8.1" : "A. Minore di 6 in tre anni",
            "2.8.2" : "B. Compreso tra 6 e 15 in tre anni",
            "2.8.3" : "C. Maggiore di 15 in tre anni",
            "2.8.4" : "D. Nessuna operazione in tre anni"
        }

        # considering mid-bins 
        bin_size = 3
        years = 3
        max = 30 / years
        bins = [round(3/years), round(10/years), round(22.5/years)]
 
        replacements = {
            '2.8.1': bins[0],
            '2.8.2': bins[1],
            '2.8.3': bins[2],
            '2.8.4': 0
        }

        array = replacements[person[column]] / max

        return max, array
    
    def v1_ordinal_subjective_time_horizon(self, person:dict):

        ##### overall info
        note = "percentuale dichiarata per orizzonte fino a 1 anno, 3 anni, 5 anni, e più di 5"

        columns = [
            'VAL_DOMANDA_S4_14_1', 'VAL_DOMANDA_S4_14_2', 
            'VAL_DOMANDA_S4_14_3', 'VAL_DOMANDA_S4_14_4'
        ]

        ##### map

        # considering max-bins
        bin_size = 4
        max_horizon = 10
        bins = [1, 3, 5, 10]

        ##### max 
        values = bins
        
        weights = []

        for column in columns:
            
            weights.append(person[column])

        weights = np.array(weights, dtype=float)
        result = np.dot(weights, values)

        array = result / max_horizon

        return max_horizon, array

    def v1_ordinal_subjective_risk(self, person:dict):

        column = 'VAL_DOMANDA_S4_15'

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

        array = replacements[person[column]]

        return max, array

    def v1_bool_sophisticated_instrument_presence(self, person:dict):

        # map
        columns = [
            "VAL_DOMANDA_S2_5A_10", "VAL_DOMANDA_S2_5A_11", "VAL_DOMANDA_S2_5A_12"
            ]

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
            if replacements[person[column]] == 1:
                result = 1
            else:
                result = 0

        return result

    def v1_ordinal_objective_time_horizon(self, person:dict):
        
        max, liquidity = self.v1_ordinal_subjective_liquidity_investment_need(person=person)
        max, capital = self.v1_ordinal_subjective_capital_accumulation_investment_need(person=person)
        max, retirement = self.v1_ordinal_subjective_retirement_investment_need(person=person)

        max_time = 10
        bins = [1, 3, 5, 10]

        if capital > 0.5:
            
            if retirement > 0.5: horizon = bins[3]
            elif retirement > 0: horizon = bins[2]
            elif retirement == 0: horizon = bins[1]

        elif capital > 0:

            if retirement > 0.5: horizon = bins[2]
            elif retirement > 0: horizon = bins[1]
            elif retirement == 0: horizon = bins[1]

        elif capital == 0:

            if retirement > 0.5: horizon = bins[2]
            elif retirement > 0: horizon = bins[1]
            elif retirement == 0: 
    
                if liquidity > 0.5: horizon = bins[2]
                elif liquidity > 0: horizon = bins[1]
                elif liquidity > 0: horizon = 0

        result = horizon

        array = result / max_time

        return max_time, array

    def v2_ordinal_correct_financial_answers(self, person:dict):


        ##### overall info
        columns = [
            'VAL_DOMANDA_S2_7_MU20',
            'VAL_DOMANDA_S2_8_MU20', 
            'VAL_DOMANDA_S2_9_MU20',
            'VAL_DOMANDA_S2_10_MU20'
        ]

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

        rating = 0
        for que, ans in true_answers.items():
            
            if replacements[person[que]] == ans:
                rating += 1

        max = len(true_answers.keys())

        array = rating / max

        return max, array


# 5) financial needs:
        
    # 5_1) investments:

    def v1_ordinal_subjective_liquidity_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_13_1"

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

        array = replacements[person[column]]

        return max, array

    def v1_ordinal_subjective_capital_accumulation_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_13_2"
        
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

        array = replacements[person[column]]
        
        return max, array

    def v1_ordinal_subjective_income_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_13_2"

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

        array = replacements[person[column]]
        
        return max, array

    def v1_ordinal_subjective_retirement_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_13_3"

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

        array = replacements[person[column]]
        
        return max, array

    def v2_ordinal_subjective_liquidity_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_13_1"

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

        array = replacements[person[column]]

        array = array / max
        
        return max, array

    def v2_ordinal_subjective_capital_accumulation_investment_need(self, person:dict):

        columns = ["VAL_DOMANDA_S4_13_2", "VAL_DOMANDA_S4_17_5_MU20"]

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

        sum = 0
        for column in columns:
            sum += replacements[person[column]]
        
        max = len(columns)

        array = sum / max
        
        return max, array

    def v2_ordinal_subjective_income_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_13_3"

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

        array = replacements[person[column]]

        array = array / max
        
        return max, array

    def v2_ordinal_subjective_capital_protection_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_17_4_MU20"

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

        array = replacements[person[column]]

        array = array / max
        
        return max, array

    def v2_ordinal_subjective_retirement_investment_need(self, person:dict):

        column = "VAL_DOMANDA_S4_17_5_MU20"

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

        array = replacements[person[column]]

        array = array / max
        
        return max, array


    # 6) personal culture:

    def v3_bool_declared_esg_propensity(self, person:dict):


        column = "VAL_DOMANDA_S5_21_MU22"

        values = {
            "A" : "A. Si, sono interessato",
            "B" : "B. No, non sono interessato"
        }

        # map
        # keys to be modified
        replacements = {
            "A" : 1,
            "B" : 0
        }

        array = replacements[person[column]]
        
        return array

    def v3_nominal_declared_esg_propensity(self, person:dict):

        column = "VAL_DOMANDA_S5_22_MU22"

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

        array = replacements[person[column]]
        
        return max, array

    def v3_bool_evironment_propensity_index(self, person:dict):
        
        # first look
        column = "VAL_DOMANDA_S5_23_MU22"

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

        if replacements[person[column]] == "A":
            rating = 1
        else:
            rating = 0

        return rating

    def v3_bool_social_propensity_index(self, person:dict):
        
        # first look
        column = "VAL_DOMANDA_S5_23_MU22"

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

        if replacements[person[column]] == "B":
            rating = 1
        else:
            rating = 0

        return rating

    def v3_bool_governance_propensity_index(self, person:dict):
        
        # first look
        column = "VAL_DOMANDA_S5_23_MU22"

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
        
        if replacements[person[column]] == "B":
            rating = 1
        else:
            rating = 0

        return rating


        