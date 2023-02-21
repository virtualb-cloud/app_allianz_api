# Allianz v2.0

import numpy as np
from app.questionnaire_contents import Contenter

class Observer:

    def __init__(self) -> None:
        
        self.contenter = Contenter()

# 1) socio demographics:

    def v1_employment_uncertainty_index(self, person:dict):

        job = self.contenter.v1_categorical_profession(person=person)

        uncertains = ["non occupato", "lavoratore dipendente"]
        
        if job in uncertains:
            flag = 1
        else:
            flag = 0
        
        return flag

    def v1_house_robbery_index(self, person:dict):

        province = person["PROV_T"]
        
        house_robbery_index = {

            'AG': 0.04649,
            'AL': 0.1287,
            'AN': 0.07773,
            'AR': 0.05942,
            'AP': 0.01987,
            'AT': 0.07532,
            'AV': 0.05617,
            'BA': 0.25506,
            'BT': 0.0311,
            'BL': 0.01227,
            'BN': 0.03942,
            'BG': 0.29383,
            'BI': 0.03201,
            'BO': 0.33948,
            'BS': 0.27422,
            'BR': 0.0774,
            'CA': 0.0724,
            'CL': 0.0337,
            'CB': 0.02565,
            'CE': 0.13084,
            'CT': 0.16474,
            'CZ': 0.02071,
            'CH': 0.04656,
            'CO': 0.15727,
            'CS': 0.07338,
            'CR': 0.08234,
            'KR': 0.00344,
            'CN': 0.15513,
            'EN': 0.00591,
            'FM': 0.03604,
            'FE': 0.06429,
            'FI': 0.31279,
            'FG': 0.07091,
            'FC': 0.11156,
            'FR': 0.05987,
            'GE': 0.16903,
            'GO': 0.00974,
            'GR': 0.05169,
            'IM': 0.05487,
            'IS': 0.0,
            'SP': 0.04974,
            'AQ': 0.03662,
            'LT': 0.11805,
            'LE': 0.15877,
            'LC': 0.09786,
            'LI': 0.06468,
            'LO': 0.03019,
            'LU': 0.15584,
            'MC': 0.04597,
            'MN': 0.07494,
            'MS': 0.05961,
            'MT': 0.0161,
            'ME': 0.03864,
            'MI': 1.0,
            'MO': 0.27883,
            'MB': 0.26318,
            'NO': 0.06247,
            'NU': 0.00851,
            'OR': 0.00221,
            'PD': 0.21468,
            'PA': 0.14058,
            'PR': 0.12117,
            'PV': 0.19032,
            'PG': 0.16929,
            'PU': 0.06403,
            'PE': 0.0526,
            'PC': 0.06117,
            'PI': 0.15305,
            'PT': 0.08883,
            'PN': 0.03818,
            'PZ': 0.02234,
            'PO': 0.05455,
            'BZ': 0.05948,
            'TN': 0.09481,
            'RG': 0.06481,
            'RA': 0.12403,
            'RC': 0.04422,
            'RE': 0.16766,
            'RI': 0.01727,
            'RN': 0.08357,
            'RM': 0.76097,
            'RO': 0.03253,
            'SA': 0.13812,
            'SS': 0.0563,
            'SV': 0.11058,
            'SI': 0.04636,
            'SR': 0.07734,
            'SO': 0.01032,
            'TA': 0.09669,
            'TR': 0.0413,
            'TE': 0.04584,
            'TO': 0.6313,
            'TP': 0.09818,
            'TV': 0.15357,
            'TS': 0.03065,
            'UD': 0.10877,
            'AO': 0.01299,
            'VA': 0.19045,
            'VE': 0.23006,
            'VB': 0.00643,
            'VC': 0.01857,
            'VR': 0.19032,
            'VV': 0.00532,
            'VI': 0.16409,
            'VT': 0.04195,
            'CI': 0.03485,
            'EE': 0.11528,
            'MZ': 0.26318,
            'VS': 0.03485,
            'OG': 0.03485,
            'OT': 0.03485,
            'RSM': 0.09756,
            'VAT': 0.76097,
            'SU': 0.03485}
        
        if province in house_robbery_index.keys():
            hri = house_robbery_index[province]
        else:
            hri = 0
        
        return hri

# 2) family status:

    def v1_family_life_quality_index(self, person:dict):

        
        ##### first look
        max, first_look = self.contenter.v1_ordinal_dependents_count(person=person)
        
        ##### second look
        max, second_look = self.contenter.v1_ordinal_childrens_count(person=person)
        
        ##### third look
        third_look = self.contenter.v1_categorical_marital_status(person=person)
        if third_look in ["Coniugato"]:
            third_look = 1
        else:
            third_look = 0
        
        ##### aggregate
        weights = [0.4, 0.3, 0.3]
        values = np.array([first_look, second_look, third_look], dtype=float)

        result = np.dot(values, weights)

        return result


# 3) financial status:

    def v1_real_asset_index(self, person:dict):

        houses = self.contenter.v1_categorical_houses_count(person=person)

        # considering mid-bins 
        bin_size = 3
        bins = [0.167, 0.500, 0.833]

        values = {
            "zero_houses" : bins[0],
            "one_house" : bins[1],
            "more_houses" : bins[2]
        }

        if houses in values.keys():
            index = values[houses]
        else:
            index = bins[0]
        
        return index

    def v1_wealth_index(self, person:dict):

        ##### first look
        max, first_look = self.contenter.v1_ordinal_out_aum(person=person)


        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_ctv(person=person)


        ##### third look
        max, third_look = self.contenter.v1_ordinal_yearly_income(person=person)
        

        ##### fourth look
        max, fourth_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        fourth_look = 1 - np.round(fourth_look/max, 2)


        ##### fifth look
        fifth_look = self.v1_real_asset_index(person=person)

        ##### aggregate
        weights = [0.2, 0.1, 0.3, 0.3, 0.2]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result
    

# 4) financial culture:

    def v1_objective_risk_index(self, person:dict):

        ##### first look
        max, first_look = self.contenter.v1_ordinal_age(person=person)


        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_income(person=person)


        ##### third look
        max, third_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        

        ##### fourth look
        max, fourth_look = self.contenter.v1_ordinal_financial_experience(person=person)
        
        ##### aggregate
        weights = [0.5, 0.2, 0.2, 0.1]
        values = np.array([first_look, second_look, third_look, fourth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_subjective_risk_index(self, person:dict):

        ##### first look
        max, first_look = self.contenter.v1_ordinal_subjective_risk(person=person)

        return first_look

    def v1_financial_litteracy_index(self, person:dict):

        ##### first look
        first_look = self.contenter.v1_categorical_education(person=person)
        if first_look in ["master di II livello e PHD"]:
            first_look = 1
        else:
            first_look = 0

        ##### second look
        max, second_look = self.contenter.v1_ordinal_financial_knowledge(person=person)

        ##### third look
        max, third_look = self.contenter.v1_ordinal_correct_financial_answers(person=person)

        ##### aggregate
        weights = [0.2, 0.3, 0.5]
        values = np.array([first_look, second_look, third_look], dtype=float)

        result = np.dot(values, weights)

        return result
   
    def v1_financial_experience_index(self, person:dict):

        ##### first look
        max, first_look = self.contenter.v1_ordinal_financial_experience(person=person)


        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_ctv(person=person)


        ##### second look
        max, third_look = self.contenter.v1_ordinal_yearly_trading_frequency(person=person)

        ##### aggregate
        weights = [0.4, 0.3, 0.3]
        values = np.array([first_look, second_look, third_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_financial_time_horizon(self, person:dict):

        ##### first look
        max, first_look = self.contenter.v1_ordinal_subjective_time_horizon(person=person)

        ##### second look
        max, second_look = self.contenter.v1_ordinal_objective_time_horizon(person=person)

        ##### aggregate
        weights = [0.7, 0.3]
        values = np.array([first_look, second_look], dtype=float)

        result = np.dot(values, weights)

        max = 10
        
        return max, result

    def v2_financial_litteracy_index(self, person:dict):

        ##### first look
        first_look = self.v1_financial_litteracy_index(person=person)

        
        ##### second look
        max, second_look = self.contenter.v2_ordinal_correct_financial_answers(person=person)
        
        ##### aggregate
        weights = [0.7, 0.3]
        values = np.array([first_look, second_look], dtype=float)

        result = np.dot(values, weights)

        return result
   

# 5) financial needs:

    # 5_1) investments:

    def v1_capital_accumulation_investment_need(self, person:dict):

        # 1) contenter.v1_ordinal_subjective_capital_accumulation_investment_need
        # 2) self.financial_time_horizon
        # 3) self.family_life_quality_index
        # 4) self.wealth_index	
        
        ##### first look
        first_look = self.contenter.v1_ordinal_subjective_capital_accumulation_investment_need(person=person)[1]
        
        ##### second look
        max, second_look = self.v1_financial_time_horizon(person=person)
        second_look = second_look / max


        ##### third look
        third_look = self.v1_family_life_quality_index(person=person)


        ##### fourth look
        fourth_look = self.v1_wealth_index(person=person)

        ##### aggregate
        weights = [0.4, 0.2, 0.2, 0.2]
        values = np.array([first_look, second_look, third_look, fourth_look], dtype=float)

        result = np.dot(values, weights)

        return result
        
    def v1_capital_protection_investment_need(self, person:dict):

        # 1) self.financial_time_horizon,
        # 2) self.objective_risk_index,
        # 3) self.subjective_risk_index,
        # 4) self.financial_litteracy_index,
        # 5) self.financial_experience_index,
        # 6) self.employment_uncertainty_index

        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max


        ##### second look
        second_look = self.v1_objective_risk_index(person=person)


        ##### third look
        third_look = self.v1_subjective_risk_index(person=person)


        ##### fourth look
        fourth_look = self.v1_financial_litteracy_index(person=person)

    
        ##### fifth look
        fifth_look = self.v1_financial_experience_index(person=person)


        ##### sixth look
        sixth_look = self.v1_employment_uncertainty_index(person=person)

        ##### aggregate
        weights = [0.2, 0.2, 0.2, 0.2, 0.1, 0.1]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look, sixth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_liquidity_investment_need(self, person:dict):

        # 1) self.family_life_quality_index
        # 2) self.wealth_index
        # 3) content.liabilities
        # 4) self.employmet_uncertainty_index 
        # 5) content.ordinal_subjective_liquidity_investment_need           			    
        
        ##### first look
        first_look = self.v1_family_life_quality_index(person=person)


        ##### second look
        second_look = self.v1_wealth_index(person=person)


        ##### third look
        max, third_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        

        ##### fourth look
        fourth_look = self.v1_employment_uncertainty_index(person=person)


        ##### fifth look
        max, fifth_look = self.contenter.v1_ordinal_subjective_liquidity_investment_need(person=person)

        ##### aggregate
        weights = [0.2, 0.1, 0.2, 0.2, 0.3]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_income_investment_need(self, person:dict):

        # 1) self.financial_time_horizon
        # 2) content.ordinal_yearly_income 
        # 3) self.wealth_index
        # 4) contenter.v1_ordinal_yearly_liabilities
        # 5) contenter.v1_ordinal_subjective_income_investment_need

        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max


        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_income(person=person)


        ##### third look
        third_look = self.v1_wealth_index(person=person)
        
    
        ##### fourth look
        max, fourth_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        

        ##### fifth look
        max, fifth_look = self.contenter.v1_ordinal_subjective_income_investment_need(person=person)

        ##### aggregate   
        weights = [0.1, 0.3, 0.1, 0.2, 0.3]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_retirement_investment_need(self, person:dict):

        # 1) self.financial_time_horizon,
        # 2) self.employment_uncertainty_index
        # 3) self.wealth_index
        # 4) content.ordinal_subjective_retirement_investment_need
            	
        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max


        ##### second look
        second_look = self.v1_employment_uncertainty_index(person=person)


        ##### third look
        third_look = self.v1_wealth_index(person=person)
        
    
        ##### fourth look
        max, fourth_look = self.contenter.v1_ordinal_subjective_retirement_investment_need(person=person)

        ##### aggregate 
        weights = [0.2, 0.2, 0.2, 0.4]
        values = np.array([first_look, second_look, third_look, fourth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_heritage_investment_need(self, person:dict):

        # 1) self.financial_time_horizon
        # 2) self.wealth_index
        # 3) content.ordinal_age

        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max

        ##### second look
        second_look = self.v1_wealth_index(person=person)

        ##### third look
        max, third_look = self.contenter.v1_ordinal_age(person=person)
        third_look = 1 - np.round(third_look/max, 2)

        ##### aggregate
        weights = [0.4, 0.3, 0.3]
        values = np.array([first_look, second_look, third_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v2_capital_accumulation_investment_need(self, person:dict):

        # 1) contenter.v2_ordinal_subjective_capital_accumulation_investment_need
        # 2) self.financial_time_horizon
        # 3) self.family_life_quality_index
        # 4) self.wealth_index	

        ##### first look
        first_look = self.contenter.v2_ordinal_subjective_capital_accumulation_investment_need(person=person)[1]
        

        ##### second look
        max, second_look = self.v1_financial_time_horizon(person=person)
        second_look = second_look / max


        ##### third look
        third_look = self.v1_family_life_quality_index(person=person)


        ##### fourth look
        fourth_look = self.v1_wealth_index(person=person)

        ##### aggregate
        weights = [0.4, 0.2, 0.2, 0.2]
        values = np.array([first_look, second_look, third_look, fourth_look], dtype=float)

        result = np.dot(values, weights)

        return result
        
    def v2_capital_protection_investment_need(self, person:dict):

        # 1) self.financial_time_horizon,
        # 2) self.objective_risk_index,
        # 3) self.subjective_risk_index,
        # 4) self.financial_litteracy_index,
        # 5) self.financial_experience_index,
        # 6) self.employment_uncertainty_index
        # 7) contenter.v2_ordinal_subjective_capital_protection_investment_need

        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max

        ##### second look
        second_look = self.v1_objective_risk_index(person=person)

        ##### third look
        third_look = self.v1_subjective_risk_index(person=person)

        ##### fourth look
        fourth_look = self.v1_financial_litteracy_index(person=person)

        ##### fifth look
        fifth_look = self.v1_financial_experience_index(person=person)

        ##### sixth look
        sixth_look = self.v1_employment_uncertainty_index(person=person)

        ##### seventh look
        max, seventh_look = self.contenter.v2_ordinal_subjective_capital_protection_investment_need(person=person)
        seventh_look = np.round(seventh_look/max, 2)

        ##### aggregate
        weights = [0.1, 0.1, 0.1, 0.1, 0.05, 0.05, 0.5]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look, sixth_look, seventh_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v2_liquidity_investment_need(self, person:dict):

        # 1) self.family_life_quality_index
        # 2) self.wealth_index
        # 3) content.liabilities
        # 4) self.employmet_uncertainty_index 
        # 5) contenter.v2_ordinal_subjective_liquidity_investment_need           			    

        ##### first look
        first_look = self.v1_family_life_quality_index(person=person)

        ##### second look
        second_look = self.v1_wealth_index(person=person)

        ##### third look
        max, third_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)

        ##### fourth look
        fourth_look = self.v1_employment_uncertainty_index(person=person)

        ##### fifth look
        max, fifth_look = self.contenter.v2_ordinal_subjective_liquidity_investment_need(person=person)
        
        ##### aggregate
        weights = [0.2, 0.1, 0.2, 0.2, 0.3]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v2_income_investment_need(self, person:dict):

        # 1) self.financial_time_horizon
        # 2) content.ordinal_yearly_income 
        # 3) self.wealth_index
        # 4) self.financial_loads_index
        # 5) contenter.v2_ordinal_subjective_income_investment_need


        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max

        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_income(person=person)

        ##### third look
        third_look = self.v1_wealth_index(person=person)
        
        ##### fourth look
        max, fourth_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        
        ##### fifth look
        max, fifth_look = self.contenter.v2_ordinal_subjective_income_investment_need(person=person)
    
        ##### aggregate
        weights = [0.1, 0.3, 0.1, 0.2, 0.3]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v2_retirement_investment_need(self, person:dict):

        # 1) self.financial_time_horizon,
        # 2) self.employment_uncertainty_index
        # 3) self.wealth_index
        # 4) contenter.v2_ordinal_subjective_retirement_investment_need
            	
        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max


        ##### second look
        second_look = self.v1_employment_uncertainty_index(person=person)


        ##### third look
        third_look = self.v1_wealth_index(person=person)
        
    
        ##### fourth look
        max, fourth_look = self.contenter.v2_ordinal_subjective_retirement_investment_need(person=person)
        
        ##### aggregate
        weights = [0.2, 0.2, 0.2, 0.4]
        values = np.array([first_look, second_look, third_look, fourth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    # 5_2) insurances:

    def v1_home_insurance_need(self, person:dict):

        # 1) self.financial_time_horizon
        # 2) self.wealth_index
        # 3) self.real_estate_index
        # 4) self.family_life_quality_index
        # 5) self.house_robbery_index

        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max


        ##### second look
        second_look = self.v1_wealth_index(person=person)


        ##### third look
        third_look = self.v1_real_asset_index(person=person)


        ##### fourth look
        fourth_look = self.v1_family_life_quality_index(person=person)


        ##### fifth look
        fifth_look = self.v1_house_robbery_index(person=person)

        ##### aggregate
        weights = [0.2, 0.2, 0.2, 0.2, 0.2]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_health_insurance_need(self, person:dict):

        # 1) self.financial_time_horizon
        # 2) self.family_life_quality_index
        # 3) content.ordinal_yearly_income
        # 4) self.wealth_index

        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max

        ##### second look
        second_look = self.v1_family_life_quality_index(person=person)

        ##### fourth look
        max, third_look = self.contenter.v1_ordinal_yearly_income(person=person)

        ##### fifth look
        fourth_look = self.v1_wealth_index(person=person)

        ##### aggregate
        weights = [0.3, 0.2, 0.3, 0.2]
        values = np.array([first_look, second_look, third_look, fourth_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_longterm_care_insurance_need(self, person:dict):

        # 1) self.financial_time_horizon
        # 2) self.wealth_index
        
        ##### first look
        max, first_look = self.v1_financial_time_horizon(person=person)
        first_look = first_look / max

        ##### second look
        second_look = self.v1_wealth_index(person=person)

        ##### aggregate
        weights = [0.7, 0.3]
        values = np.array([first_look, second_look], dtype=float)

        result = np.dot(values, weights)

        return result
    
    # 5_3) financing:

    def v1_payment_financing_need(self, person:dict):

        # 1) content.ordinal_yearly_income
        # 2) content.ordinal_yearly_liabilities

        ##### first look
        max, first_look = self.contenter.v1_ordinal_yearly_income(person=person)
        first_look = 1 - np.round(first_look/max, 2)

        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        second_look = 1 - np.round(second_look/max, 2)

        ##### aggregate
        weights = [0.7, 0.3]
        values = np.array([first_look, second_look], dtype=float)

        result = np.dot(values, weights)

        return result
        
    def v1_loan_financing_need(self, person:dict):

        # 1) content.ordinal_yearly_income
        # 2) content.ordinal_yearly_liabilities
        # 3) self.family_life_quality_index

        ##### first look
        max, first_look = self.contenter.v1_ordinal_yearly_income(person=person)

        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        second_look = 1 - np.round(second_look/max, 2)

        ##### third look
        third_look = self.v1_family_life_quality_index(person=person)

        ##### aggregate
        weights = [0.3, 0.5, 0.2]
        values = np.array([first_look, second_look, third_look], dtype=float)

        result = np.dot(values, weights)

        return result

    def v1_mortgage_financing_need(self, person:dict):

        # 1) content.ordinal_yearly_income
        # 2) content.ordinal_yearly_liabilities
        # 3) self.real_asset_index
        
        ##### first look
        max, first_look = self.contenter.v1_ordinal_yearly_income(person=person)

        ##### second look
        max, second_look = self.contenter.v1_ordinal_yearly_liabilities(person=person)
        second_look = 1 - np.round(second_look/max, 2)

        ##### third look
        third_look = self.v1_real_asset_index(person=person)

        ##### aggregate
        weights = [0.1, 0.4, 0.5]
        values = np.array([first_look, second_look, third_look], dtype=float)

        result = np.dot(values, weights)

        return result

# 6) personal culture:
 
    def v3_esg_propensity_index(self, person:dict):

        # 1) contenter.v3_bool_declared_esg_propensity
        # 2) contenter.v3_nominal_declared_esg_propensity
        # 3) contenter.v3_bool_evironment_propensity_index
        # 4) contenter.v3_bool_social_propensity_index	
        # 5) contenter.v3_bool_governance_propensity_index

        ##### first look
        first_look = self.contenter.v3_bool_declared_esg_propensity(person=person)

        ##### second look
        max, second_look = self.contenter.v3_nominal_declared_esg_propensity(person=person)

        ##### third look
        third_look = self.contenter.v3_bool_evironment_propensity_index(person=person)

        ##### fourth look
        fourth_look = self.contenter.v3_bool_social_propensity_index(person=person)

        ##### fifth look
        fifth_look = self.contenter.v3_bool_governance_propensity_index(person=person)
        
        ##### aggregate
        weights = [0.3, 0.4, 0.1, 0.1, 0.1]
        values = np.array([first_look, second_look, third_look, fourth_look, fifth_look], dtype=float)

        result = np.dot(values, weights)

        return result