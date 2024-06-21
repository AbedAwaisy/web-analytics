import pandas as pd
from integrations.Helpper_Functions import *


class Meta_Extraction:
    def __init__(self, df):
        self.df = df
        self.columns = columns_mapper(self.df)

    def extract_Meta(self):
        SampleID = self.df[self.columns['מזהה דגימה']].astype(int)
        ParcelID = self.df[self.columns['חלקה']].astype(int)
        SortingDate = pd.to_datetime(self.df[self.columns['תאריך מיון']], errors='coerce')
        HarvestDate = pd.to_datetime(self.df[self.columns['תאריך קטיף']], errors='coerce').dt.date
        HerbName = self.df['HerbName'].astype(str)
        ParcelSize = self.df[self.columns['גודל חלקה במר']].astype(float)
        SortingType = self.df[self.columns['סוג מיון']].astype(str)

        meta = pd.DataFrame(
            {'SampleID': SampleID, 'ParcelID': ParcelID, 'SortingDate': SortingDate, 'HarvestDate': HarvestDate,
             'HerbName': HerbName, 'ParcelSize': ParcelSize, 'SortingType': SortingType}, index=self.df.index)
        meta['SortingType'] = meta['SortingType'].apply(lambda x: x.replace(" ", "").lower())
        self.df = meta
        return meta