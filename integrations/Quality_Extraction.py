class Quality_Extraction:
    def __init__(self, df, experiment=None):
        self.df = df
        self.exp = experiment.strip()
        self.columns = columns_mapper(self.df)

    def extract_Quality(self):
        result_columns = ['משקל weight', 'רעננות שדרהvine freshness', 'נפליםfallen', 'מפוצציםcracked', 'מוצקיםfirm',
                          'גמישיםflexible', 'רכיםsoft', 'רקוביםrotten', 'חריגי צבעcolor defect', 'רקבון שזרהvine rot',
                          'חסריםmissing', 'גווןshade', 'מראה כלליgeneral appearance', 'וירוס צבעcolor virus',
                          'וירוס שריטותscratches virus']
        ordinal_columns = []
        for c in result_columns:
            self.df = remove_outliers_iqr(self.df, self.columns[c])

        Quality_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int),
                                   'Weight': self.df[self.columns['משקל weight']].astype(float),
                                   'FruietNumber': self.df[self.columns['מספר פרות']].astype(int),
                                   'VineFreshness': self.df[self.columns['רעננות שדרהvine freshness']].astype(int),
                                   'Fallen': self.df[self.columns['נפליםfallen']].astype(int),
                                   'Cracked': self.df[self.columns['מפוצציםcracked']].astype(int),
                                   'Frim': self.df[self.columns['מוצקיםfirm']].astype(int),
                                   'Flexible': self.df[self.columns['גמישיםflexible']].astype(int),
                                   'Soft': self.df[self.columns['רכיםsoft']].astype(int),
                                   'Rotten': self.df[self.columns['רקוביםrotten']].astype(int),
                                   'ColorDefect': self.df[self.columns['חריגי צבעcolor defect']].astype(int),
                                   'Missing': self.df[self.columns['חסריםmissing']].astype(int),
                                   'VineRot': self.df[self.columns['רקבון שזרהvine rot']].astype(float),
                                   'Shade': self.df[self.columns['גווןshade']].astype(float),
                                   'GeneralAppearance': self.df[self.columns['מראה כלליgeneral appearance']].astype(
                                       float),
                                   'ColorVirus': self.df[self.columns['וירוס צבעcolor virus']].astype(float),
                                   'ScratchesVirus': self.df[self.columns['וירוס שריטותscratches virus']].astype(
                                       float)}, index=self.df.index)

        if self.exp:
            self.df['ExperimentType'] = self.exp
            if self.df[self.columns[self.exp]].isna().sum() > 0:
                self.df[self.columns[self.exp]].fillna('Control', inplace=True)

            self.df['ExperimentParameter'] = self.df[self.columns[self.exp]]
            experiment_cols = {'ExperimentType': self.df['ExperimentType'].astype(str),
                               'ExperimentParameter': self.df['ExperimentParameter'].astype(str)}
            first_column_index = Quality_df.columns.get_loc(Quality_df.columns[0])
            # Add the new columns after the first column
            for col_name, col_data in experiment_cols.items():
                Quality_df.insert(first_column_index + 1, col_name, col_data)

            Quality_df = cap_values(Quality_df,
                                    ['Fallen', 'Cracked', 'Frim', 'Flexible', 'Soft', 'Rotten', 'ColorDefect',
                                     'Missing'], 'FruietNumber')

        return Quality_df
