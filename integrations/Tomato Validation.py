import pandas as pd
from datetime import datetime
from collections import defaultdict
from Helpper_Functions import *
import math
import mysql.connector
import numpy as np


if __name__ == "__main__":
    # Example Usage:
    # tometo1/Experiment163Results.csv
    data = pd.read_csv('C:/Users/ASUS/Desktop/tometo1/Experiment108Results.csv')
    experiment_user = input("Please enter Experiment column name if there is an Experiment in the file: ")
    exp = ' '.join(experiment_user.split())
    columns = columns_mapper(data)
    data[columns['סוג מיון']] = data[columns['סוג מיון']].apply(lambda x: x.replace(" ", "").lower())
    
    validator_meta = Validation_Meta(data)
    validator_meta.validate_sampleID()
    validator_meta.validate_harvest_date()
    validator_meta.validate_sorting_date()
    validator_meta.validate_date_gap()
    validator_meta.validate_parcel_size()
    validator_meta.validate_exists_herb_name_info()
    validator_meta.validate_herb_name_info_with_dbms()
    
    Meta_Extractor = Meta_Extraction(validator_meta.df)
    meta = Meta_Extractor.extract_Meta()
    
    c, q, y = None, None, None
    sorting_types = list(validator_meta.df[columns['סוג מיון']].unique())
    sorting_types = [s.replace(" ", "").lower() for s in sorting_types]
    for s in sorting_types:
        if s in ['cemical', 'כימי', 'cemicalכימי', 'כימיcemical']:
            c_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
            Cemical_Extractor = Cemical_Extraction(c_df, exp)
            c = Cemical_Extractor.extract_Cemical()
            
        elif s in ['quality', 'איכות', 'qualityאיכות', 'איכותquality']:
            q_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
            Validateor_Quality = Validation_Quality(q_df)
            Validateor_Quality.validate_wieght()
            Validateor_Quality.validate_vine_root()
            Validateor_Quality.validate_vine_freshness()
            Validateor_Quality.validate_shade()
            Validateor_Quality.validate_scratches_virus()
            Validateor_Quality.validate_general_appereance()
            Validateor_Quality.validate_color_virus()
            Quality_Extractor = Quality_Extraction(Validateor_Quality.df, exp)
            q = Quality_Extractor.extract_Quality()
            
        elif s in ['yield', 'רגיל', 'yieldרגיל', 'רגילyield']:
            y_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
            Validateor_Yield = Validation_Yield(y_df)
            Validateor_Yield.validate_single_harvest()
            Validateor_Yield.validate_single_harvest_number()
            Yield_Extractor = Yield_Extraction(Validateor_Yield.df, exp)
            y = Yield_Extractor.extract_Yield()
            
        else:
            raise ValueError(f'error sorting type {s} not integrated with the sysytem')
    

    meta_data = Meta_to_keep(c, q, y, meta)       
    Inserter = Insertion(meta_data, yield_df=y, quality_df=q, cemical_df=c, exp=exp)    
    Inserter.insert()       
    
    



