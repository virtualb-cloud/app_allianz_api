# Allianz v2.0

import json
import numpy as np
import pandas as pd
import requests
from scipy.stats import beta
from app.questionnaire_contents import Contenter
from app.questionnaire_observations import Observer
from app.ingestion_utils import Type_Null_controller


class Customers:

    def __init__(self, external_df:pd.DataFrame) -> None:

        # copy
        self.external_df = external_df

        # questionnaire modules initialization
        self.questionnaire_versions = ["MIFID_1_0", "MIFID_2_0", "MIFID_3_0"]
        self.version_column = "TIPO_MIFID"
        self.contenter = Contenter()
        self.observer = Observer()

        # columns
        self.mifid1_columns_type = {
            "SOGGETTO" : "float", 
            'ETA' : "float", 
            "ISR" : "float", 
            'OTS' : "float", 
            'VAL_DOMANDA_S4_14_1' : "float", 
            'VAL_DOMANDA_S4_14_2' : "float", 
            'VAL_DOMANDA_S4_14_3' : "float", 
            'VAL_DOMANDA_S4_14_4' : "float",
            'SESSO_B' : "string", 
            'PROV_T' : "string", 
            'PROFILO_SINT_RICHIED_N' : "string", 
            'PROFESSIONE_S' : "string",
            'TAE_T' : "string", 
            'ESPERIENZA_DES' : "string",
            'TIPO_MIFID' : "string", 
            'VAL_DOMANDA_S1_1' : "string", 
            'VAL_DOMANDA_S1_2' : "string",
            'VAL_DOMANDA_S1_3' : "string", 
            'VAL_DOMANDA_S1_4' : "string", 
            'VAL_DOMANDA_S2_5A_1' : "string", 
            'VAL_DOMANDA_S2_5A_2' : "string",
            'VAL_DOMANDA_S2_5A_3' : "string", 
            'VAL_DOMANDA_S2_5A_4' : "string", 
            'VAL_DOMANDA_S2_5A_5' : "string",
            'VAL_DOMANDA_S2_5A_6' : "string", 
            'VAL_DOMANDA_S2_5A_7' : "string",
            'VAL_DOMANDA_S2_5A_8' : "string", 
            'VAL_DOMANDA_S2_5A_9' : "string", 
            'VAL_DOMANDA_S2_5A_10' : "string", 
            'VAL_DOMANDA_S2_5A_11' : "string", 
            'VAL_DOMANDA_S2_5A_12' : "string", 
            'VAL_DOMANDA_S2_5B_1' : "string", 
            'VAL_DOMANDA_S2_5B_2' : "string", 
            'VAL_DOMANDA_S2_5B_3' : "string", 
            'VAL_DOMANDA_S2_5B_4' : "string",
            'VAL_DOMANDA_S2_5B_5' : "string", 
            'VAL_DOMANDA_S2_5B_6' : "string", 
            'VAL_DOMANDA_S2_5B_7' : "string", 
            'VAL_DOMANDA_S2_5B_8' : "string", 
            'VAL_DOMANDA_S2_5B_9' : "string", 
            'VAL_DOMANDA_S2_5B_10' : "string", 
            'VAL_DOMANDA_S2_5B_11' : "string", 
            'VAL_DOMANDA_S2_5B_12' : "string", 
            'VAL_DOMANDA_S2_6_1' : "string", 
            'VAL_DOMANDA_S2_6_2' : "string", 
            'VAL_DOMANDA_S2_6_3' : "string", 
            'VAL_DOMANDA_S2_6_4' : "string", 
            'VAL_DOMANDA_S2_7' : "string", 
            'VAL_DOMANDA_S2_8' : "string",
            'VAL_DOMANDA_S3_9' : "string", 
            'VAL_DOMANDA_S3_10' : "string", 
            'VAL_DOMANDA_S3_11' : "string",
            'VAL_DOMANDA_S3_12' : "string", 
            'VAL_DOMANDA_S4_13_1' : "string", 
            'VAL_DOMANDA_S4_13_2' : "string",
            'VAL_DOMANDA_S4_13_3' : "string", 
            'VAL_DOMANDA_S4_15' : "string", 
            "DATA_INSE_CLI" : "string", 
            "DATA_CENSIMENTO" : "string"
            }

        self.mifid2_columns_type = {
            'NASCITA_FIGLIO_1_MU20' : "integer", 
            'NASCITA_FIGLIO_2_MU20' : "integer",
            'NASCITA_FIGLIO_3_MU20' : "integer", 
            'NASCITA_FIGLIO_4_MU20' : "integer",
            'NASCITA_FIGLIO_5_MU20' : "integer", 
            'NASCITA_FIGLIO_6_MU20' : "integer",
            'VAL_DOMANDA_S2_7_MU20' : "string",
            'VAL_DOMANDA_S2_8_MU20' : "string", 
            'VAL_DOMANDA_S2_9_MU20' : "string",
            'VAL_DOMANDA_S2_10_MU20' : "string", 
            'VAL_DOMANDA_S4_17_4_MU20' : "string",
            'VAL_DOMANDA_S4_17_5_MU20' : "string", 
            'VAL_DOMANDA_S4_18_MU20' : "string"
            }

        self.mifid3_columns_type = {
            'VAL_DOMANDA_S5_21_MU22' : "string",
            'VAL_DOMANDA_S5_22_MU22' : "string", 
            'VAL_DOMANDA_S5_23_MU22' : "string"
            }

# synthetic variables

    def _ordinal_subjective_risk_index_synthetic(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df

        column = "ISR"

        # pre-fitted beta distribution
        a=9
        b=0.3
        for idx, row in temp_df.iterrows():
            distribution = beta.ppf(temp_df.loc[idx, column], a, b)
            temp_df.loc[idx, column] = np.round(distribution, 4)

        return temp_df[column]

    def _ordinal_financial_experience_index(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df
    
        column = "ESPERIENZA_DES"

        # considering mid-bins
        bin_size = 3
        bins = [0.167, 0.500, 0.833]

        replacements = {
            'ALTA' : bins[0],
            'MEDIA' : bins[1],
            'BASSA' : bins[2]
            }

        return temp_df[column].map(replacements)

    def _ordinal_subjective_risk_index_categorical(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df
   
        column = "PROFILO_SINT_RICHIED_N"

        # considering mid-bins
        bin_size = 5
        bins = [0.1, 0.3, 0.5, 0.7, 0.9]

        replacements = {
            'CONSERVATIVO' : 0,
            'PRUDENTE' : bins[0],
            'EQUILIBRATO' : bins[1],
            'EVOLUTO' : bins[2],
            'DINAMICO' : bins[3],
            }

        return temp_df[column].map(replacements)

    def _ordinal_synthetic_time_horizon(self) -> pd.Series:
        
        # copy
        temp_df = self.initialized_df

        column = "OTS"

        return temp_df[column]

# questionnaire

    def mifid_2018_kpi(self, version_separated_df:pd.DataFrame) -> pd.DataFrame:

        # copy
        temp_df = version_separated_df

        people = []

        for idx, row in temp_df:

            person = {
                    "id" : f'customer_{temp_df.loc[idx, "SOGGETTO"]}',
                    "sociodemographics" : {
                        "age" : np.round(self.contenter.v1_ordinal_age(temp_df) * 100),
                        "gender" : self.contenter.v1_categorical_gender(temp_df),
                        "location" : self.contenter.v1_categorical_location_region(temp_df),
                        "education" : self.contenter.v1_categorical_education(temp_df),
                        "profession" : self.contenter.v1_categorical_profession(temp_df)
                    },
                    "status" : {
                        "net_income_index" : self.contenter.v1_ordinal_yearly_income(temp_df),
                        "financial_assets_index" : self.contenter.v1_ordinal_out_aum(temp_df),
                        "real_assets_index" : self.observer.v1_real_asset_index(temp_df),
                        "net_liabilities_index" : self.contenter.v1_ordinal_yearly_liabilities(temp_df),
                        "net_wealth_index" :  self.observer.v1_wealth_index(temp_df)
                    },
                    "cultures" : {
                        "objective_risk_index" : self.observer.v1_objective_risk_index(temp_df),
                        "subjective_risk_index" : self.observer.v1_subjective_risk_index(temp_df),
                        "financial_litteracy_index" : self.observer.v1_financial_litteracy_index(temp_df),
                        "financial_horizon_index" : self.observer.v1_financial_time_horizon(temp_df),
                        "financial_experience_index" : self.observer.v1_financial_experience_index(temp_df),
                        "life_quality_index" : self.observer.v1_family_life_quality_index(temp_df),
                        "sophisticated_instrument" : self.contenter.v1_bool_sophisticated_instrument_presence(temp_df)
                    },
                    "needs" : {
                        "capital_accumulation_investment_need" : self.observer.v1_capital_accumulation_investment_need(temp_df),
                        "capital_protection_investment_need" : self.observer.v1_capital_protection_investment_need(temp_df),
                        "liquidity_investment_need" : self.observer.v1_liquidity_investment_need(temp_df),
                        "income_investment_need" : self.observer.v1_income_investment_need(temp_df),
                        "retirement_investment_need" : self.observer.v1_retirement_investment_need(temp_df),
                        "heritage_investment_need" : self.observer.v1_heritage_investment_need(temp_df),
                        "home_insurance_need" : self.observer.v1_home_insurance_need(temp_df),
                        "health_insurance_need" : self.observer.v1_health_insurance_need(temp_df),
                        "longterm_care_insurance_need" : self.observer.v1_longterm_care_insurance_need(temp_df),
                        "payment_financing_need" : self.observer.v1_payment_financing_need(temp_df),
                        "loan_financing_need" : self.observer.v1_loan_financing_need(temp_df),
                        "mortgage_financing_need" : self.observer.v1_mortgage_financing_need(temp_df)
                    }
                }

            people.append(person)

        return people

    def mifid_2020_kpi(self, version_separated_df:pd.DataFrame) -> pd.DataFrame:
        
        
        # copy
        temp_df = version_separated_df

        people = []

        for idx, row in temp_df:

            person = {
                    "id" : f'customer_{temp_df.loc[idx, "SOGGETTO"]}',
                    "sociodemographics" : {
                        "age" : np.round(self.contenter.v1_ordinal_age(temp_df) * 100),
                        "gender" : self.contenter.v1_categorical_gender(temp_df),
                        "location" : self.contenter.v1_categorical_location_region(temp_df),
                        "education" : self.contenter.v1_categorical_education(temp_df),
                        "profession" : self.contenter.v1_categorical_profession(temp_df)
                    },
                    "status" : {
                        "net_income_index" : self.contenter.v1_ordinal_yearly_income(temp_df),
                        "financial_assets_index" : self.contenter.v1_ordinal_out_aum(temp_df),
                        "real_assets_index" : self.observer.v1_real_asset_index(temp_df),
                        "net_liabilities_index" : self.contenter.v1_ordinal_yearly_liabilities(temp_df),
                        "net_wealth_index" :  self.observer.v1_wealth_index(temp_df)
                    },
                    "cultures" : {
                        "objective_risk_index" : self.observer.v1_objective_risk_index(temp_df),
                        "subjective_risk_index" : self.observer.v1_subjective_risk_index(temp_df),
                        "financial_litteracy_index" : self.observer.v2_financial_litteracy_index(temp_df),
                        "financial_horizon_index" : self.observer.v1_financial_time_horizon(temp_df),
                        "financial_experience_index" : self.observer.v1_financial_experience_index(temp_df),
                        "life_quality_index" : self.observer.v1_family_life_quality_index(temp_df),
                        "sophisticated_instrument" : self.contenter.v1_bool_sophisticated_instrument_presence(temp_df)
                    },
                    "needs" : {
                        "capital_accumulation_investment_need" : self.observer.v2_capital_accumulation_investment_need(temp_df),
                        "capital_protection_investment_need" : self.observer.v2_capital_protection_investment_need(temp_df),
                        "liquidity_investment_need" : self.observer.v2_liquidity_investment_need(temp_df),
                        "income_investment_need" : self.observer.v2_income_investment_need(temp_df),
                        "retirement_investment_need" : self.observer.v2_retirement_investment_need(temp_df),
                        "heritage_investment_need" : self.observer.v1_heritage_investment_need(temp_df),
                        "home_insurance_need" : self.observer.v1_home_insurance_need(temp_df),
                        "health_insurance_need" : self.observer.v1_health_insurance_need(temp_df),
                        "longterm_care_insurance_need" : self.observer.v1_longterm_care_insurance_need(temp_df),
                        "payment_financing_need" : self.observer.v1_payment_financing_need(temp_df),
                        "loan_financing_need" : self.observer.v1_loan_financing_need(temp_df),
                        "mortgage_financing_need" : self.observer.v1_mortgage_financing_need(temp_df)
                    }
                }

            people.append(person)

        return people

    def mifid_2022_kpi(self, version_separated_df:pd.DataFrame) -> pd.DataFrame:

        # copy
        temp_df = version_separated_df
                # copy
        temp_df = version_separated_df

        people = []

        for idx, row in temp_df:

            person = {
                    "id" : f'customer_{temp_df.loc[idx, "SOGGETTO"]}',
                    "sociodemographics" : {
                        "age" : np.round(self.contenter.v1_ordinal_age(temp_df) * 100),
                        "gender" : self.contenter.v1_categorical_gender(temp_df),
                        "location" : self.contenter.v1_categorical_location_region(temp_df),
                        "education" : self.contenter.v1_categorical_education(temp_df),
                        "profession" : self.contenter.v1_categorical_profession(temp_df)
                    },
                    "status" : {
                        "net_income_index" : self.contenter.v1_ordinal_yearly_income(temp_df),
                        "financial_assets_index" : self.contenter.v1_ordinal_out_aum(temp_df),
                        "real_assets_index" : self.observer.v1_real_asset_index(temp_df),
                        "net_liabilities_index" : self.contenter.v1_ordinal_yearly_liabilities(temp_df),
                        "net_wealth_index" :  self.observer.v1_wealth_index(temp_df)
                    },
                    "cultures" : {
                        "objective_risk_index" : self.observer.v1_objective_risk_index(temp_df),
                        "subjective_risk_index" : self.observer.v1_subjective_risk_index(temp_df),
                        "financial_litteracy_index" : self.observer.v2_financial_litteracy_index(temp_df),
                        "financial_horizon_index" : self.observer.v1_financial_time_horizon(temp_df),
                        "financial_experience_index" : self.observer.v1_financial_experience_index(temp_df),
                        "life_quality_index" : self.observer.v1_family_life_quality_index(temp_df),
                        "esg_propensity_index" : self.observer.v3_esg_propensity_index(temp_df),
                        "sophisticated_instrument" : self.contenter.v1_bool_sophisticated_instrument_presence(temp_df)
                    },
                    "needs" : {
                        "capital_accumulation_investment_need" : self.observer.v2_capital_accumulation_investment_need(temp_df),
                        "capital_protection_investment_need" : self.observer.v2_capital_protection_investment_need(temp_df),
                        "liquidity_investment_need" : self.observer.v2_liquidity_investment_need(temp_df),
                        "income_investment_need" : self.observer.v2_income_investment_need(temp_df),
                        "retirement_investment_need" : self.observer.v2_retirement_investment_need(temp_df),
                        "heritage_investment_need" : self.observer.v1_heritage_investment_need(temp_df),
                        "home_insurance_need" : self.observer.v1_home_insurance_need(temp_df),
                        "health_insurance_need" : self.observer.v1_health_insurance_need(temp_df),
                        "longterm_care_insurance_need" : self.observer.v1_longterm_care_insurance_need(temp_df),
                        "payment_financing_need" : self.observer.v1_payment_financing_need(temp_df),
                        "loan_financing_need" : self.observer.v1_loan_financing_need(temp_df),
                        "mortgage_financing_need" : self.observer.v1_mortgage_financing_need(temp_df)
                    }
                }

            people.append(person)

        return people

    def questionnaire_handler(self) -> pd.DataFrame:

        # copy
        temp_df = self.external_df
        
        # initiation
        version_separated_df = pd.DataFrame()
        mapped_df = pd.DataFrame()

        data = []
        
        for version in self.questionnaire_versions:

            version_separated_df = temp_df[temp_df[self.version_column] == version]

            # if empty go to next version
            if version_separated_df.empty: continue

            # for version 2018
            if version == self.questionnaire_versions[0]:

                keys = self.mifid1_columns_type

                # initialization
                initializer = Type_Null_controller(keys)
                response = initializer.run(version_separated_df)
                if response[0] == False: print(response)
                else: self.initialized_df = response[1]

                people = self.mifid_2018_kpi(self.initialized_df)

                body = json.dumps(people)

                headers = {
                    "Content-Type": "application/json"
                }

                response = requests.put(
                    url="https://customers-dialogue.herokuapp.com/insert_customers",
                    data=body,
                    headers=headers
                    )


            # for version 2020
            elif version == self.questionnaire_versions[1]: 
                
                keys = self.mifid1_columns_type
                for key in self.mifid2_columns_type:
                    keys[key] = self.mifid2_columns_type[key]

                # initialization
                initializer = Type_Null_controller(key)
                response = initializer.run(version_separated_df)
                if response[0] == False: print(response)
                else: self.initialized_df = response[1]

                people = self.mifid_2020_kpi(self.initialized_df)

                body = json.dumps(people)

                headers = {
                    "Content-Type": "application/json"
                }

                response = requests.put(
                    url="https://customers-dialogue.herokuapp.com/insert_customers",
                    data=body,
                    headers=headers
                    )

            # for version 2022
            elif version == self.questionnaire_versions[2]: 
                
                keys = self.mifid1_columns_type
                for key in self.mifid2_columns_type:
                    keys[key] = self.mifid2_columns_type[key]
                for key in self.mifid3_columns_type:
                    keys[key] = self.mifid3_columns_type[key]

                # initialization
                initializer = Type_Null_controller(keys)
                response = initializer.run(version_separated_df)
                if response[0] == False: print(response)
                else: self.initialized_df = response[1]
                
                people = self.mifid_2022_kpi(self.initialized_df)

                body = json.dumps(people)

                headers = {
                    "Content-Type": "application/json"
                }

                response = requests.put(
                    url="https://customers-dialogue.herokuapp.com/insert_customers",
                    data=body,
                    headers=headers
                    )

        return mapped_df



    def run(self):
        
        self.questionnaire_handler()

        return True