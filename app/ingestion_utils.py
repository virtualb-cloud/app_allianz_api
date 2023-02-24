# ALLIANZ V2_0

import numpy as np
import pandas as pd


class Type_Null_controller:

    def __init__(self, columns_expected_type:dict) -> tuple:
        
        self.columns_expected_type = columns_expected_type

        self.integer_annotation = "integer"
        self.float_annotation = "float"
        self.string_annotation = "string"
        self.datetime_annotation = "datetime"

    def control_keys(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        for column in self.columns_expected_type.keys():

            if not column in local_df.columns: 
                flag = False
                break

        return flag, column

    def null_filler_median_based(self, array:pd.Series) -> pd.Series:
        
        # impose null
        median = array.median()

        sign = array.isnull()

        for idx, val in sign.items():

            if val == True:

                array.at[idx] = median

        return array

    def null_filler_mode_based(self, array:pd.Series) -> pd.Series:
        
        mode = array.mode()[0]

        # impose null with mode
        sign = array.isnull()

        for idx, val in sign.items():

            if val == True:

                array.at[idx] = mode

        array = array.apply(lambda x: mode if x == 'nan' else x)
        array = array.apply(lambda x: mode if x == '--' else x)

        return array
            
    def null_filler_average_based(self, array:pd.Series) -> pd.Series:

        average = array.mean()

        # impose null with mean
        sign = array.isnull()

        for idx, val in sign.items():

            if val == True:

                array.at[idx] = average

        return array

    def impose_integer(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        try :

            for column in self.columns_expected_type.keys():

                if self.columns_expected_type[column] == self.integer_annotation:
                    
                    # impose null
                    local_df[column] = self.null_filler_median_based(local_df[column])

                    # impose type
                    local_df[column] = local_df[column].astype(int)


            return flag, local_df

        except:

            flag = False
            
            return flag, column 

    def impose_float(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        try:

            for column in self.columns_expected_type.keys():
                
                if self.columns_expected_type[column] == self.float_annotation:
                    
                    if local_df[column].dtype != float:
                    
                        # impose type
                        local_df[column] = local_df[column].apply(lambda x: str(x).replace('.',''))
                        local_df[column] = local_df[column].apply(lambda x: str(x).replace(',','.'))
                        local_df[column] = local_df[column].astype(float)

                    # impose null
                    local_df[column] = self.null_filler_median_based(local_df[column])

            return flag, local_df

        except:

            flag = False
            
            return flag, column 
            
    def impose_string(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        try:

            for column in self.columns_expected_type.keys():
            
                if self.columns_expected_type[column] == self.string_annotation:
                    
                    # impose type
                    local_df[column] = local_df[column].astype(str)

                    # impose null
                    local_df[column] = self.null_filler_mode_based(local_df[column])

            return flag, local_df

        except:

            flag = False
            
            return flag, column 

    def impose_datetime(self, local_df:pd.DataFrame) -> tuple:
        
        flag = True

        try:

            for column in self.columns_expected_type.keys():
                
                if self.columns_expected_type[column] == self.datetime_annotation:
                    
                    # impose type
                    local_df[column] = pd.to_datetime(local_df[column], dayfirst=True, infer_datetime_format=True)

            return flag, local_df

        except:

            flag = False
            
            return flag, column 

    def run(self, df:pd.DataFrame):

        # flag for job
        flag = True

        #### check keys
        response = self.control_keys(df)
        if response[0] == False: return False, response[1], "control-keys"

        #### call the imposers
        response = self.impose_integer(df)
        if response[0] == False: return False, response[1], "impose-integer"
        
        response = self.impose_float(response[1])
        if response[0] == False: return False, response[1], "impose-float"
        
        response = self.impose_string(response[1])
        if response[0] == False: return False, response[1], "impose-string"
        
        response = self.impose_datetime(response[1])
        if response[0] == False: return False, response[1], "impose-datetime"
        
        
        return True, response[1], "DONE"


class Agreggator:

    def __init__(self) -> tuple:
        
        pass

    def build_beta_binomial_distribution(self, sample_mean:float, sample_power:int) -> list:
        
        # alpha and beta calculation
        alpha = sample_power * sample_mean
        beta = sample_power - alpha

        return alpha, beta

    def beta_binomial_aggregator(self, distributions:list) -> float:
        # info on distributions: [(sample_mean, sample_power), (,), ...]
        
        # empty variables for summation of distribution variables
        alphas, betas = 0, 0

        # find parameters
        for element in distributions:
            alpha, beta = self.build_beta_binomial_distribution(element[0], element[1])
            alphas += alpha
            betas += beta

        peak_of_distribution = alphas / (alphas + betas)

        return peak_of_distribution
