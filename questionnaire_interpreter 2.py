# Allianz v2.0

import json
import numpy as np
import pandas as pd
from scipy.stats import beta
from questionnaire_contents import Contenter
from questionnaire_observations import Observer
from ingestion_utils import Type_Null_controller


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


# questionnaire

    def mifid_2018_kpi(self, version_separated_df:pd.DataFrame) -> pd.DataFrame:

        # copy
        temp_df = version_separated_df
        local_df = pd.DataFrame()

        # customer and date tag        
        local_df["allianz_id"] = temp_df["SOGGETTO"]
        local_df["inserting_date"] = temp_df["DATA_INSE_CLI"]

        # 1) GD:
        
        max, local_df["age"] = self.contenter.v1_ordinal_age(temp_df)
        local_df["age"] = local_df["age"] * max
        local_df["gender"] = self.contenter.v1_categorical_gender(temp_df)
        local_df["location"] = self.contenter.v1_categorical_location_region(temp_df)
        local_df["education"] = self.contenter.v1_categorical_education(temp_df)
        local_df["profession"] = self.contenter.v1_categorical_profession(temp_df)
        
        # 2) family status:
        local_df["life_quality_index"] = self.observer.v1_family_life_quality_index(temp_df)
        
        
        # 3) financial status:
        
        max, local_df["net_income_index"] = self.contenter.v1_ordinal_yearly_income(temp_df)
        max, local_df["financial_assets_index"] = self.contenter.v1_ordinal_out_aum(temp_df)
        max, local_df["net_liabilities_index"] = self.contenter.v1_ordinal_yearly_liabilities(temp_df)
        local_df["real_assets_index"] = self.observer.v1_real_asset_index(temp_df)
        local_df["net_wealth_index"] = self.observer.v1_wealth_index(temp_df)

        # 4) financial culture:
        
        local_df["objective_risk_index"] = self.observer.v1_objective_risk_index(temp_df)
        local_df["subjective_risk_index"] = self.observer.v1_subjective_risk_index(temp_df)
        local_df["financial_litteracy_index"] = self.observer.v1_financial_litteracy_index(temp_df)
        max, local_df["financial_horizon_index"] = self.observer.v1_financial_time_horizon(temp_df)
        local_df["financial_experience_index"] = self.observer.v1_financial_experience_index(temp_df)


        # 5) financial needs:

        #investments:
        local_df["capital_accumulation_investment_need"] = self.observer.v1_capital_accumulation_investment_need(temp_df)
        local_df["capital_protection_investment_need"] = self.observer.v1_capital_protection_investment_need(temp_df)
        local_df["liquidity_investment_need"] = self.observer.v1_liquidity_investment_need(temp_df)
        local_df["income_investment_need"] = self.observer.v1_income_investment_need(temp_df)
        local_df["retirement_investment_need"] = self.observer.v1_retirement_investment_need(temp_df)
        local_df["heritage_investment_need"] = self.observer.v1_heritage_investment_need(temp_df)
        
        
        #insurances:
        local_df["home_insurance_need"] = self.observer.v1_home_insurance_need(temp_df)
        local_df["health_insurance_need"] = self.observer.v1_health_insurance_need(temp_df)
        local_df["longterm_care_insurance_need"] = self.observer.v1_longterm_care_insurance_need(temp_df)

        #financings:
        local_df["payment_financing_need"] = self.observer.v1_payment_financing_need(temp_df)
        local_df["loan_financing_need"] = self.observer.v1_loan_financing_need(temp_df)
        local_df["mortgage_financing_need"] = self.observer.v1_mortgage_financing_need(temp_df)

        return local_df

    def mifid_2020_kpi(self, version_separated_df:pd.DataFrame) -> pd.DataFrame:
        
        
        # copy
        temp_df = version_separated_df
        local_df = pd.DataFrame()

        # customer and date tag        
        local_df["allianz_id"] = temp_df["SOGGETTO"]
        local_df["inserting_date"] = temp_df["DATA_INSE_CLI"]

        # 1) GD:

        max, local_df["age"] = self.contenter.v1_ordinal_age(temp_df)
        local_df["age"] = local_df["age"] * max
        local_df["gender"] = self.contenter.v1_categorical_gender(temp_df)
        local_df["location"] = self.contenter.v1_categorical_location_region(temp_df)
        local_df["education"] = self.contenter.v1_categorical_education(temp_df)
        local_df["profession"] = self.contenter.v1_categorical_profession(temp_df)

        # 2) family status:
        local_df["life_quality_index"] = self.observer.v1_family_life_quality_index(temp_df)

        # 3) financial status:

        max, local_df["net_income_index"] = self.contenter.v1_ordinal_yearly_income(temp_df)
        max, local_df["financial_assets_index"] = self.contenter.v1_ordinal_out_aum(temp_df)
        max, local_df["net_liabilities_index"] = self.contenter.v1_ordinal_yearly_liabilities(temp_df)
        local_df["net_wealth_index"] = self.observer.v1_wealth_index(temp_df)
        local_df["real_assets_index"] = self.observer.v1_real_asset_index(temp_df)

        # 4) financial culture:
        
        local_df["objective_risk_index"] = self.observer.v1_objective_risk_index(temp_df)
        local_df["subjective_risk_index"] = self.observer.v1_subjective_risk_index(temp_df)
        local_df["financial_litteracy_index"] = self.observer.v2_financial_litteracy_index(temp_df)
        max, local_df["financial_time_horizon"] = self.observer.v1_financial_time_horizon(temp_df)
        local_df["financial_experience_index"] = self.observer.v1_financial_experience_index(temp_df)

        # 5) financial needs:

        #investments:
        local_df["capital_accumulation_investment_need"] = self.observer.v2_capital_accumulation_investment_need(temp_df)
        local_df["capital_protection_investment_need"] = self.observer.v2_capital_protection_investment_need(temp_df)
        local_df["liquidity_investment_need"] = self.observer.v2_liquidity_investment_need(temp_df)
        local_df["income_investment_need"] = self.observer.v2_income_investment_need(temp_df)
        local_df["retirement_investment_need"] = self.observer.v2_retirement_investment_need(temp_df)
        local_df["heritage_investment_need"] = self.observer.v1_heritage_investment_need(temp_df)

        
        #insurances:
        local_df["home_insurance_need"] = self.observer.v1_home_insurance_need(temp_df)
        local_df["health_insurance_need"] = self.observer.v1_health_insurance_need(temp_df)
        local_df["longterm_care_insurance_need"] = self.observer.v1_longterm_care_insurance_need(temp_df)


        #financings:
        local_df["payment_financing_need"] = self.observer.v1_payment_financing_need(temp_df)
        local_df["loan_financing_need"] = self.observer.v1_loan_financing_need(temp_df)
        local_df["mortgage_financing_need"] = self.observer.v1_mortgage_financing_need(temp_df)

        return local_df

    def mifid_2022_kpi(self, version_separated_df:pd.DataFrame) -> pd.DataFrame:

        # copy
        temp_df = version_separated_df
        local_df = pd.DataFrame()

        # customer and date tag        
        local_df["allianz_id"] = temp_df["SOGGETTO"]
        local_df["inserting_date"] = temp_df["DATA_INSE_CLI"]

        # 1) GD:

        max, local_df["age"] = self.contenter.v1_ordinal_age(temp_df)
        local_df["age"] = local_df["age"] * max
        local_df["gender"] = self.contenter.v1_categorical_gender(temp_df)
        local_df["location"] = self.contenter.v1_categorical_location_region(temp_df)
        local_df["education"] = self.contenter.v1_categorical_education(temp_df)
        local_df["profession"] = self.contenter.v1_categorical_profession(temp_df)

        # 2) family status:
        local_df["family_life_quality_index"] = self.observer.v1_family_life_quality_index(temp_df)

        # 3) financial status:

        max, local_df["net_income_index"] = self.contenter.v1_ordinal_yearly_income(temp_df)
        max, local_df["financial_assets_index"] = self.contenter.v1_ordinal_out_aum(temp_df)
        max, local_df["net_liabilities_index"] = self.contenter.v1_ordinal_yearly_liabilities(temp_df)
        local_df["net_wealth_index"] = self.observer.v1_wealth_index(temp_df)
        local_df["real_assets_index"] = self.observer.v1_real_asset_index(temp_df)
        
        # 4) financial culture:
        
        local_df["objective_risk_index"] = self.observer.v1_objective_risk_index(temp_df)
        local_df["subjective_risk_index"] = self.observer.v1_subjective_risk_index(temp_df)
        local_df["financial_litteracy_index"] = self.observer.v2_financial_litteracy_index(temp_df)
        max, local_df["financial_time_horizon"] = self.observer.v1_financial_time_horizon(temp_df)
        local_df["financial_experience_index"] = self.observer.v1_financial_experience_index(temp_df)

        # 5) financial needs:

        #investments:
        local_df["capital_accumulation_investment_need"] = self.observer.v2_capital_accumulation_investment_need(temp_df)
        local_df["capital_protection_investment_need"] = self.observer.v2_capital_protection_investment_need(temp_df)
        local_df["liquidity_investment_need"] = self.observer.v2_liquidity_investment_need(temp_df)
        local_df["income_investment_need"] = self.observer.v2_income_investment_need(temp_df)
        local_df["retirement_investment_need"] = self.observer.v2_retirement_investment_need(temp_df)
        local_df["heritage_investment_need"] = self.observer.v1_heritage_investment_need(temp_df)

        
        #insurances:
        local_df["home_insurance_need"] = self.observer.v1_home_insurance_need(temp_df)
        local_df["health_insurance_need"] = self.observer.v1_health_insurance_need(temp_df)
        local_df["longterm_care_insurance_need"] = self.observer.v1_longterm_care_insurance_need(temp_df)


        #financings:
        local_df["payment_financing_need"] = self.observer.v1_payment_financing_need(temp_df)
        local_df["loan_financing_need"] = self.observer.v1_loan_financing_need(temp_df)
        local_df["mortgage_financing_need"] = self.observer.v1_mortgage_financing_need(temp_df)

        # 6) personal culture:

        local_df["esg_propensity_index"] = self.observer.v3_esg_propensity_index(temp_df)
        local_df["evironment_propensity_index"] = self.contenter.v3_bool_evironment_propensity_index(temp_df)
        local_df["social_propensity_index"] = self.contenter.v3_bool_social_propensity_index(temp_df)
        local_df["governance_propensity_index"] = self.contenter.v3_bool_governance_propensity_index(temp_df)
        
        return local_df

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

                data.append(self.mifid_2018_kpi(self.initialized_df))

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

                data.append(self.mifid_2020_kpi(self.initialized_df))

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

                
                data.append(self.mifid_2022_kpi(self.initialized_df))

        # join the data
        for item in data:
            mapped_df = pd.concat([mapped_df, item], axis=0, ignore_index=True)

        return mapped_df


# map

    def read_map_customer(self):
        
        # read the map
        with open('pipelines_ingestion/maps/map_customer.json') as json_file:
            mappa = json.load(json_file)

        return mappa

# final tables

    def hub_customer(self):
        
        # ingest questionnaire
        self.ready_df = self.questionnaire_handler()

        #### copy
        temp_df = self.ready_df

        # customer sociodemographics to db
        socio_demographics = [
            "age", "gender", 
            "location", "education", "profession"
        ]

        # create primary key
        temp_df = temp_df.reset_index()
        temp_df["customer_id"] = temp_df.index + 1

        #### write the map
        customers_list = temp_df[["customer_id", "allianz_id"]].to_dict(orient="records")
        mappa = dict()

        # build
        for customer in customers_list:
            
            key = tuple()

            for identity in customer:
                if identity == "allianz_id":
                    key = (*key, customer[identity])

            mappa[str(key)] = customer["customer_id"]
        
        # save
        with open("pipelines_ingestion/maps/map_customer.json", "w") as write_file:
            json.dump(mappa, write_file, indent=4)

        # set
        temp_df.set_index("customer_id", inplace=True)

        return temp_df[socio_demographics]

    def sat_customer_culture(self):
        
        #### copy
        temp_df = self.ready_df

        # desired columns
        financial_culture = [ 
            "allianz_id", "objective_risk_index", "subjective_risk_index", 
            "financial_litteracy_index", "financial_time_horizon",
            "financial_experience_index", "trading_frequency",
            "sophisticated_instrument", "family_life_quality_index"
        ]
        
        # create foreign key
        mappa = self.read_map_customer()

        for idx, row in temp_df.iterrows():
            
            key = tuple()

            # make the key
            key = (*key, row["allianz_id"])

            # ask the id
            temp_df.loc[idx, "customer_id"] = mappa[str(key)] if str(key) in mappa.keys() else np.nan

        temp_df.set_index("customer_id", inplace=True)
        
        return temp_df[financial_culture]

    def sat_customer_status(self):

        #### copy

        temp_df = self.ready_df

        # desired columns
        financial_status = [
            "allianz_id", "net_income_index", "out_aum", "net_liabilities_index"
        ]

        # create foreign key
        mappa = self.read_map_customer()

        for idx, row in temp_df.iterrows():
            
            key = tuple()

            # make the key
            key = (*key, row["allianz_id"])

            # ask the id
            temp_df.loc[idx, "customer_id"] = mappa[str(key)]

        temp_df.set_index("customer_id", inplace=True)

        return temp_df[financial_status]

    def sat_customer_needs(self):

        #### copy

        temp_df = self.ready_df

        # desired columns
        financial_needs = [
            "allianz_id",
            "capital_accumulation_investment_need",
            "capital_protection_investment_need",
            "liquidity_investment_need",
            "income_investment_need",
            "retirement_investment_need",
            "heritage_investment_need",
            "home_insurance_need",
            "health_insurance_need",
            "longterm_care_insurance_need",
            "payment_financing_need",
            "loan_financing_need",
            "mortgage_financing_need"
        ]

        # create foreign key
        mappa = self.read_map_customer()

        for idx, row in temp_df.iterrows():
            
            key = tuple()

            # make the key
            key = (*key, row["allianz_id"])

            # ask the id
            temp_df.loc[idx, "customer_id"] = mappa[str(key)]
            
        temp_df.set_index("customer_id", inplace=True)

        return temp_df[financial_needs]

    def run(self):
        
        self.hub_customer()

        self.sat_customer_culture()

        self.sat_customer_needs()

        self.sat_customer_status

        return True