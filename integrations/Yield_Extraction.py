import pandas as pd
from integrations.Helpper_Functions import *


class Yield_Extraction:
    def __init__(self, df, experiment=None):
        self.df = df
        self.exp = experiment.strip()
        self.columns = columns_mapper(self.df)

    def extract_Yield(self):
        result_columns = ['יצוא אשכולות', 'יצוא בודדים', 'יצוא בודדים מספר', 'ירוקים', 'סדוקים', 'שחור פיטם',
                          'שחור פיטם מספר', 'אחרים', 'הגנת הצומח', 'וירוס']
        self.df = FruietNumber_Integrator(self.df, self.columns['יצוא בודדים מספר'])

        for c in result_columns:
            self.df = remove_outliers_iqr(self.df, self.columns[c])

        Yield_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int),
                                 'ClusterHarvesd': self.df[self.columns['יצוא אשכולות']].astype(float),
                                 'SingleHarvesd': self.df[self.columns['יצוא בודדים']].astype(float),
                                 'SingleHarvesdNumber': self.df[self.columns['יצוא בודדים מספר']],
                                 'GreenFruits': self.df[self.columns['ירוקים']].astype(float),
                                 'CrackedFruits': self.df[self.columns['סדוקים']].astype(float),
                                 'BlackPitDefect': self.df[self.columns['שחור פיטם']].astype(float),
                                 'BlackPitDefectNumber': self.df[self.columns['שחור פיטם מספר']].astype(int),
                                 'Others': self.df[self.columns['אחרים']].astype(float),
                                 'PlantProtection': self.df[self.columns['הגנת הצומח']].astype(float),
                                 'Virus': self.df[self.columns['וירוס']].astype(float)}, index=self.df.index)
        if self.exp:
            self.df['ExperimentType'] = self.exp
            if self.df[self.columns[self.exp]].isna().sum() > 0:
                self.df[self.columns[self.exp]].fillna('Control', inplace=True)

            self.df['ExperimentParameter'] = self.df[self.columns[self.exp]]
            experiment_cols = {'ExperimentType': self.df['ExperimentType'].astype(str),
                               'ExperimentParameter': self.df['ExperimentParameter'].astype(str)}
            first_column_index = Yield_df.columns.get_loc(Yield_df.columns[0])
            # Add the new columns after the first column
            for col_name, col_data in experiment_cols.items():
                Yield_df.insert(first_column_index + 1, col_name, col_data)

        return Yield_df