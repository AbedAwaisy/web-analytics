import pandas as pd
from integrations.Helpper_Functions import *


class Cemical_Extraction:
    def __init__(self, df, experiment=None):
        self.df = df
        self.exp = experiment.strip()
        self.columns = columns_mapper(self.df)

    def extract_Cemical(self):
        self.df = remove_outliers_zscore(self.df, self.columns['TA'])
        self.df = remove_outliers_zscore(self.df, self.columns['TSS'])
        self.df = remove_outliers_iqr(self.df, self.columns['גלוקוז'])

        if self.exp:
            self.df['ExperimentType'] = self.exp
            if self.df[self.columns[self.exp]].isna().sum() > 0:
                self.df[self.columns[self.exp]].fillna('Control', inplace=True)

            self.df['ExperimentParameter'] = self.df[self.columns[self.exp]]
            cemical_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int),
                                       'ExperimentType': self.df['ExperimentType'].astype(str),
                                       'ExperimentParameter': self.df['ExperimentParameter'].astype(str),
                                       'TSS': self.df[self.columns['TSS']].astype(float),
                                       'TA': self.df[self.columns['TA']].astype(float),
                                       'Glucose': self.df[self.columns['גלוקוז']].astype(float)}, index=self.df.index)
        else:
            cemical_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int),
                                       'TSS': self.df[self.columns['TSS']].astype(float),
                                       'TA': self.df[self.columns['TA']].astype(float),
                                       'Glucose': self.df[self.columns['גלוקוז']].astype(float)}, index=self.df.index)

        return cemical_df