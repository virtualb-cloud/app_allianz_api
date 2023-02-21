# Allianz v2.0

import json
from scipy.stats import beta
from app.questionnaire_contents import Contenter
from app.questionnaire_observations import Observer

import requests
import json

class Questionnaire:

    def __init__(self) -> None:

        # questionnaire modules initialization
        self.contenter = Contenter()
        self.observer = Observer()


    def enrich(self, person:dict):

        max, age = self.contenter.v1_ordinal_age(person)
        age = int(age * max)
        gender = self.contenter.v1_categorical_gender(person)
        location = self.contenter.v1_categorical_location_region(person)
        education = self.contenter.v1_categorical_education(person)
        profession = self.contenter.v1_categorical_profession(person)

        body = [{
            "id" : person["id"],
            "age" : age,
            "gender" : gender,
            "location" : location,
            "education" : education,
            "profession" : profession
        }]

        answer = {}

        answer["cultures"] = json.loads(requests.post("https://enrichment.herokuapp.com/predict_cultures", json=body, timeout=60).text)[0]
        answer["status"] = json.loads(requests.post("https://enrichment.herokuapp.com/predict_status", json=body, timeout=60).text)[0]
        answer["attitudes"] = json.loads(requests.post("https://enrichment.herokuapp.com/predict_attitudes", json=body, timeout=60).text)[0]
        answer["needs"] = json.loads(requests.post("https://enrichment.herokuapp.com/predict_needs", json=body, timeout=60).text)[0]

        return answer
        
    def needs(self, person:dict):

        questionnaire_type = person["TIPO_MIFID"]

        try: 
            if questionnaire_type in ["MIFID_2_0", "MIFID_3_0"]:
                capital_accumulation_investment_need = self.observer.v2_capital_accumulation_investment_need(person)

            elif questionnaire_type == "MIFID_1_0":
                capital_accumulation_investment_need = self.observer.v1_capital_accumulation_investment_need(person)

        except: capital_accumulation_investment_need = self.enriched["needs"]["capital_accumulation_investment_need"]

        try: 
            if questionnaire_type in ["MIFID_2_0", "MIFID_3_0"]:
                capital_protection_investment_need = self.observer.v2_capital_protection_investment_need(person)
            
            elif questionnaire_type == "MIFID_1_0":
                capital_protection_investment_need = self.observer.v1_capital_protection_investment_need(person)

        except: capital_protection_investment_need = self.enriched["needs"]["capital_protection_investment_need"]
            
        try: 
            if questionnaire_type in ["MIFID_2_0", "MIFID_3_0"]:
                liquidity_investment_need = self.observer.v2_liquidity_investment_need(person)
            
            elif questionnaire_type == "MIFID_1_0":
                liquidity_investment_need = self.observer.v1_liquidity_investment_need(person)

        except: liquidity_investment_need = self.enriched["needs"]["liquidity_investment_need"]
            
        try: 
            if questionnaire_type in ["MIFID_2_0", "MIFID_3_0"]:   
                income_investment_need = self.observer.v2_income_investment_need(person)
            
            elif questionnaire_type == "MIFID_1_0":
                income_investment_need = self.observer.v1_income_investment_need(person)

        except: income_investment_need = self.enriched["needs"]["income_investment_need"]
            
        try: 
            if questionnaire_type in ["MIFID_2_0", "MIFID_3_0"]:  
                retirement_investment_need = self.observer.v2_retirement_investment_need(person)
            
            elif questionnaire_type == "MIFID_1_0":
                retirement_investment_need = self.observer.v1_retirement_investment_need(person)

        except: retirement_investment_need = self.enriched["needs"]["retirement_investment_need"]
            
        try: heritage_investment_need = self.observer.v1_heritage_investment_need(person)
        except: heritage_investment_need = self.enriched["needs"]["heritage_investment_need"]
        
        try: home_insurance_need = self.observer.v1_home_insurance_need(person)
        except: home_insurance_need = self.enriched["needs"]["home_insurance_need"]
        
        try: health_insurance_need = self.observer.v1_health_insurance_need(person)
        except: health_insurance_need = self.enriched["needs"]["health_insurance_need"]
        
        try: longterm_care_insurance_need = self.observer.v1_longterm_care_insurance_need(person)
        except: longterm_care_insurance_need = self.enriched["needs"]["longterm_care_insurance_need"]
        
        try: payment_financing_need = self.observer.v1_payment_financing_need(person)
        except: payment_financing_need = self.enriched["needs"]["payment_financing_need"]
        
        try: loan_financing_need = self.observer.v1_loan_financing_need(person)
        except: loan_financing_need = self.enriched["needs"]["loan_financing_need"]
        
        try: mortgage_financing_need = self.observer.v1_mortgage_financing_need(person)
        except: mortgage_financing_need = self.enriched["needs"]["mortgage_financing_need"]

        answer = {
                "capital_accumulation_investment_need" : round(capital_accumulation_investment_need, 2),
                "capital_protection_investment_need" : round(capital_protection_investment_need, 2),
                "liquidity_investment_need" : round(liquidity_investment_need, 2),
                "income_investment_need" : round(income_investment_need, 2),
                "retirement_investment_need" : round(retirement_investment_need, 2),
                "heritage_investment_need" : round(heritage_investment_need, 2),
                "home_insurance_need" : round(home_insurance_need, 2),
                "health_insurance_need" : round(health_insurance_need, 2),
                "longterm_care_insurance_need" : round(longterm_care_insurance_need, 2),
                "payment_financing_need" : round(payment_financing_need, 2),
                "loan_financing_need" : round(loan_financing_need, 2),
                "mortgage_financing_need" : round(mortgage_financing_need, 2)
            }
        return answer

    def cultures(self, person:dict):
        
        questionnaire_type = person["TIPO_MIFID"]

        try: 
            life_quality_index = self.observer.v1_family_life_quality_index(person)
        except: life_quality_index = self.enriched["cultures"]["life_quality_index"]

        try: 
            objective_risk_index = self.observer.v1_objective_risk_index(person)
        except: objective_risk_index = self.enriched["cultures"]["objective_risk_index"]
            
        try: 
            subjective_risk_index = self.observer.v1_subjective_risk_index(person)
        except: subjective_risk_index = self.enriched["cultures"]["subjective_risk_index"]
            
        try: 
            if questionnaire_type in ["MIFID_2_0", "MIFID_3_0"]:   
                financial_litteracy_index = self.observer.v2_financial_litteracy_index(person)
            
            elif questionnaire_type == "MIFID_1_0":
                financial_litteracy_index = self.observer.v1_financial_litteracy_index(person)

        except: financial_litteracy_index = self.enriched["cultures"]["financial_litteracy_index"]
            
        try: max, financial_horizon_index = self.observer.v1_financial_time_horizon(person)
        except: financial_horizon_index = self.enriched["cultures"]["financial_horizon_index"]
        
        try: 
            financial_experience_index = self.observer.v1_financial_experience_index(person)
        except: financial_experience_index = self.enriched["cultures"]["financial_experience_index"]

        #try: 
        if questionnaire_type in ["MIFID_3_0"]:
            esg_propensity_index = self.observer.v3_esg_propensity_index(person)
        else: 
            esg_propensity_index = self.enriched["cultures"]["esg_propensity_index"]

        try: complex_instrument = self.contenter.v1_bool_sophisticated_instrument_presence(person)
        except: complex_instrument = 0

        answer = {
                "life_quality_index" : round(life_quality_index, 2),
                "objective_risk_index" : round(objective_risk_index, 2),
                "subjective_risk_index" : round(subjective_risk_index, 2),
                "financial_litteracy_index" : round(financial_litteracy_index, 2),
                "financial_experience_index" : round(financial_experience_index, 2),
                "financial_horizon_index" : round(financial_horizon_index, 2),
                "esg_propensity_index" : round(esg_propensity_index, 2),
                "complex_instrument" : round(complex_instrument, 2)
            }

        return answer

    def status(self, person:dict):
        
        questionnaire_type = person["TIPO_MIFID"]

        try: net_income_index = self.contenter.v1_ordinal_yearly_income(person)[1]
        except: net_income_index = self.enriched["status"]["net_income_index"]

        try: financial_assets_index = self.contenter.v1_ordinal_out_aum(person)[1]
        except: financial_assets_index = self.enriched["status"]["financial_assets_index"]
            
        try: net_liabilities_index = self.contenter.v1_ordinal_yearly_liabilities(person)[1]
        except: net_liabilities_index = self.enriched["status"]["net_liabilities_index"]
            
        try: net_wealth_index = self.observer.v1_wealth_index(person)
        except: net_wealth_index = self.enriched["status"]["net_wealth_index"]
        
        try: real_assets_index = self.observer.v1_real_asset_index(person)
        except: real_assets_index = self.enriched["status"]["real_assets_index"]

        net_savings_index = self.enriched["status"]["net_savings_index"]
        
        answer = {
                "real_assets_index" : round(real_assets_index, 2),
                "financial_assets_index" : round(financial_assets_index, 2),
                "net_liabilities_index" : round(net_liabilities_index, 2),
                "net_wealth_index" : round(net_wealth_index, 2),
                "net_income_index" : round(net_income_index, 2),
                "net_savings_index" : round(net_savings_index, 2)
            }
            
        return answer

    def attitudes(self, person:dict):
        
        questionnaire_type = person["TIPO_MIFID"]

        bank_activity_index = self.enriched["attitudes"]["bank_activity_index"]

        digital_activity_index = self.enriched["attitudes"]["digital_activity_index"]
            
        cultural_activity_index = self.enriched["attitudes"]["cultural_activity_index"]

        charity_activity_index = self.enriched["attitudes"]["charity_activity_index"]

        answer = {
                "digital_activity_index" : round(digital_activity_index, 2),
                "bank_activity_index" : round(bank_activity_index, 2),
                "cultural_activity_index" : round(cultural_activity_index, 2),
                "charity_activity_index" : round(charity_activity_index, 2)
            }
            
        return answer

    def run(self, people:list):

        outputs = []

        for person in people:
            
            self.enriched = self.enrich(person=person)

            output = {
                "id" : person["id"],
                "cultures" : self.cultures(person),
                "attitudes" : self.attitudes(person),
                "status" : self.status(person),
                "needs" : self.needs(person)
            }
            outputs.append(output)

        return outputs
