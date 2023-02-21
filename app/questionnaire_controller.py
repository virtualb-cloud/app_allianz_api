# ALLIANZ V2_0

class Mifid_controller:

    def __init__(self) -> None:
        
        # columns
        self.mifid1_columns = {
            'ETA' : [n for n in range(0, 121, 1)],
            'SESSO_B' : ["", "MASCHILE", "FEMMINILE"], 
            'PROV_T' : [
                "", 'AO', 'TO', 'NO', 'VB', 'AL', 'VC', 'AT', 'BI', 
                'CN', 'LO', 'CO', 'MI', 'MN', 'VA', 'MB', 'BS', 
                'BG', 'CR', 'SO', 'PV', 'PN', 'GO', 'TS', 'UD', 
                'PI', 'AR', 'FI', 'SI', 'LU', 'PO', 'PT', 'MS', 
                'GR', 'LI', 'SV', 'GE', 'IM', 'SP', 'TV', 'VE', 
                'VI', 'VR', 'PD', 'BL', 'RI', 'RO', 'TR', 'PG', 
                'TE', 'AQ', 'PE', 'CH', 'TN', 'BZ', 'RN', 'RE', 
                'PR', 'RA', 'BO', 'FE', 'PC', 'MO', 'FC', 'FR', 
                'VT', 'LT', 'RM', 'AN', 'FM', 'MC', 'PU', 'AP', 
                'PS', 'CE', 'SA', 'AV', 'BN', 'IS', 'CB', 'PZ', 
                'MT', 'CZ', 'CS', 'KR', 'VV', 'RC', 'BA', 'FG', 
                'BT', 'TA', 'LE', 'BR', 'PA', 'CT', 'ME', 'RG', 
                'CL', 'EN', 'SR', 'TP', 'AG', 'CA', 'SS', 'SU', 
                'OT', 'NU', 'OR', 'OG', 'CI', 'EE', 'ZH', 'UK', "LC"
                ], 
            'PROFESSIONE_S' : [
                "", '01 DIRIGENTE', '02 IMPRENDITORE',
                '03 LAVORATORE AUTONOMO', '04 LIBERO PROFESSIONISTA',
                '05 PENSIONATO', '06 LAV. DIPENDENTE A TEMPO DETERMINATO',
                '07 LAV. DIPENDENTE A TEMPO INDETERMINATO',
                '08 NON OCCUPATO (STUDENTI/CASALINGHE)'
                ],
            'VAL_DOMANDA_S1_1' : ["", "1.1.1", "1.1.2", "1.1.3", "1.1.4"], 
            'VAL_DOMANDA_S1_2' : ["", "1.2.1", "1.2.2", "1.2.3", "1.2.4"],
            'VAL_DOMANDA_S1_3' : ["", "1.3.1", "1.3.2", "1.3.3", "1.3.4"], 
            'VAL_DOMANDA_S1_4' : ["", "1.4.1", "1.4.2", "1.4.3", "1.4.4"], 
            'VAL_DOMANDA_S2_5A_1' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_2' : ["", "S", "N"],
            'VAL_DOMANDA_S2_5A_3' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_4' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_5' : ["", "S", "N"],
            'VAL_DOMANDA_S2_5A_6' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_7' : ["", "S", "N"],
            'VAL_DOMANDA_S2_5A_8' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_9' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_10' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_11' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5A_12' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_1' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_2' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_3' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_4' : ["", "S", "N"],
            'VAL_DOMANDA_S2_5B_5' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_6' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_7' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_8' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_9' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_10' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_11' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_5B_12' : ["", "S", "N"], 
            'VAL_DOMANDA_S2_6_1' : ["", "A", "B"], 
            'VAL_DOMANDA_S2_6_2' : ["", "A", "B"], 
            'VAL_DOMANDA_S2_6_3' : ["", "A", "B"], 
            'VAL_DOMANDA_S2_6_4' : ["", "A", "B"], 
            'VAL_DOMANDA_S2_7' : ["", "2.7.1", "2.7.2", "2.7.3", "2.7.4"], 
            'VAL_DOMANDA_S2_8' : ["", "2.8.1", "2.8.2", "2.8.3", "2.8.4"],
            'VAL_DOMANDA_S3_9' : ["", "3.9.1", "3.9.2", "3.9.11", "3.9.12", "3.9.13", "3.9.14"], 
            'VAL_DOMANDA_S3_10' : ["", "3.10.1", "3.10.2", "3.10.3", "3.10.4", "3.10.5", "3.10.6", "3.10.11", "3.10.12", "3.10.13", "3.10.14", "3.10.15"], 
            'VAL_DOMANDA_S3_11' : ["", "3.11.1", "3.11.2", "3.11.3", "3.11.11", "3.11.12", "3.11.13"],
            'VAL_DOMANDA_S3_12' : ["", "3.12.1", "3.12.2", "3.12.3", "3.12.4"], 
            'VAL_DOMANDA_S4_13_1' : ["", "P", "I", "N"], 
            'VAL_DOMANDA_S4_13_2' : ["", "P", "I", "N"],
            'VAL_DOMANDA_S4_13_3' : ["", "P", "I", "N"], 
            'VAL_DOMANDA_S4_14_1' : [round(n/100, 2) for n in range(0, 101, 1)], 
            'VAL_DOMANDA_S4_14_2' : [round(n/100, 2) for n in range(0, 101, 1)], 
            'VAL_DOMANDA_S4_14_3' : [round(n/100, 2) for n in range(0, 101, 1)], 
            'VAL_DOMANDA_S4_14_4' : [round(n/100, 2) for n in range(0, 101, 1)],
            'VAL_DOMANDA_S4_15' : ["", "4.15.1", "4.15.2", "4.15.3", "4.15.4"]

            }

        self.mifid2_columns = {
            'NASCITA_FIGLIO_1_MU20' : [n for n in range(1980, 2023, 1)] + [0], 
            'NASCITA_FIGLIO_2_MU20' : [n for n in range(1980, 2023, 1)] + [0],
            'NASCITA_FIGLIO_3_MU20' : [n for n in range(1980, 2023, 1)] + [0], 
            'NASCITA_FIGLIO_4_MU20' : [n for n in range(1980, 2023, 1)] + [0],
            'NASCITA_FIGLIO_5_MU20' : [n for n in range(1980, 2023, 1)] + [0], 
            'NASCITA_FIGLIO_6_MU20' : [n for n in range(1980, 2023, 1)] + [0],
            'VAL_DOMANDA_S2_7_MU20' : ["", "2.9.1", "2.9.2", "2.9.3"],
            'VAL_DOMANDA_S2_8_MU20' : ["", "2.10.1", "2.10.2", "2.10.3"], 
            'VAL_DOMANDA_S2_9_MU20' : ["", "2.11.1", "2.11.2", "2.11.3"],
            'VAL_DOMANDA_S2_10_MU20' : ["", "2.12.1", "2.12.2", "2.12.3"], 
            'VAL_DOMANDA_S4_17_4_MU20' : ["", "P", "I", "N"],
            'VAL_DOMANDA_S4_17_5_MU20' : ["", "P", "I", "N"],
            'VAL_DOMANDA_S4_18_MU20' : ["", "4.16.11", "4.16.12", "4.16.13", "4.16.14", "4.16.15"]
            }

        self.mifid3_columns = {
            'VAL_DOMANDA_S5_21_MU22' : ["", "A", "B"],
            'VAL_DOMANDA_S5_22_MU22' : ["", "A", "B", "C"],
            'VAL_DOMANDA_S5_23_MU22' : ["", "A", "B", "C"],
            }

        self.questionnaire_versions = ["MIFID_1_0", "MIFID_2_0", "MIFID_3_0"]

    def control_mifid_1(self, person:dict) -> bool:
        
        flag = True

        for key, value in self.mifid1_columns.items():

            if not key in person.keys(): 
                flag = False
                break

            if not person[key] in value:
                flag = False
                break

        return flag, key, value

    def control_mifid_2(self, person:dict) -> bool:
        
        flag = True

        for key, value in self.mifid2_columns.items():

            if not key in person.keys(): 
                flag = False
                break
            
            if not person[key] in value:
                flag = False
                break

        return flag, key, value

    def control_mifid_3(self, person:dict) -> bool:
        
        flag = True

        for key, value in self.mifid3_columns.items():

            if not key in person.keys(): 
                flag = False
                break
            
            if not person[key] in value:
                flag = False
                break

        return flag, key, value

    def run(self, people:list) -> bool:
        
        new_people = []

        errors = ""

        for person in people:

            try:
                id = person["id"]
                mifid = person["TIPO_MIFID"]
            except:
                errors += "in body, consider 'id' and 'TIPO_MIFID' with values: " + str(self.questionnaire_versions) + ". "
                continue
            
            if mifid == "MIFID_1_0": 
                response = self.control_mifid_1(person=person)
                if not response[0]:
                    errors += f"error at key: {response[1]}, considering values: {response[2]}"
                    continue
                else:
                    new_people.append(person)

            elif mifid == "MIFID_2_0": 
                
                response = self.control_mifid_1(person=person)
                if not response[0]:
                    errors += f"error at key: {response[1]}, considering values: {response[2]}"
                    continue
                else:
                    response = self.control_mifid_2(person=person)
                    if not response[0]:
                        errors += f"error at key: {response[1]}, considering values: {response[2]}"
                        continue
                    else:
                        new_people.append(person)

            elif mifid == "MIFID_3_0": 
                
                response = self.control_mifid_1(person=person)
                if not response[0]:
                    errors += f"error at key: {response[1]}, considering values: {response[2]}"
                    continue
                
                else:
                    response = self.control_mifid_2(person=person)
                    if not response[0]:
                        errors += f"error at key: {response[1]}, considering values: {response[2]}"
                        continue
                    else:
                        response = self.control_mifid_3(person=person)
                        if not response[0]:
                            errors += f"error at key: {response[1]}, considering values: {response[2]}"
                            continue

                        else:
                            new_people.append(person)
            
            else:
                errors += "in body, consider 'TIPO_MIFID' with values: " + str(self.questionnaire_versions) + ". "

        return new_people, errors
