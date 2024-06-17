class Validation_Yield:
    def __init__(self, df, single_harvest_number_thresh=0.5, single_harvest_thresh=0.5):
        self.columns = columns_mapper(df)
        self.df = update_single_harvest_number(df, self.columns['יצוא בודדים'], self.columns['יצוא בודדים מספר'])
        self.n = len(self.df)
        self.thresh1 = single_harvest_number_thresh
        self.thresh2 = single_harvest_thresh

    def validate_single_harvest(self):
        col = self.columns['יצוא בודדים']
        data = self.df[self.df[col] != 0]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        if number_of_unique_values / n < self.thresh2:
            authority = input(
                f"the system guesses that there is invalid values in the single harvest column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values: ")
            if authority != 'yes':
                raise ValueError(f"Invalid single harvest value(s) founded in the single harvest column ({col}).")

    def validate_single_harvest_number(self):
        col = self.columns['יצוא בודדים מספר']
        data_filtered = self.df[self.df[col] != 0]
        data = data_filtered[data_filtered != -1]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        if number_of_unique_values / n < self.thresh1:
            authority = input(
                f"the system guesses that there is invalid values in the single harvest number column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values: ")
            if authority != 'yes':
                raise ValueError(
                    f"Invalid single harvest value(s) founded in the single harvest number column ({col}).")


