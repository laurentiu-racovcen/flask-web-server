import pandas

class DataIngestor:
    def __init__(self, csv_path: str):
        self.__questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.__questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of '
            'moderate-intensity aerobic physical activity or 75 minutes a week of '
            'vigorous-intensity aerobic activity (or an equivalent combination)',

            'Percent of adults who achieve at least 150 minutes a week of '
            'moderate-intensity aerobic physical activity or 75 minutes a week of '
            'vigorous-intensity aerobic physical activity and engage in '
            'muscle-strengthening activities on 2 or more days a week',

            'Percent of adults who achieve at least 300 minutes a week of '
            'moderate-intensity aerobic physical activity or 150 minutes a week of '
            'vigorous-intensity aerobic activity (or an equivalent combination)',

            'Percent of adults who engage in muscle-strengthening activities '
            'on 2 or more days a week'
        ]

        # read csv from csv_path
        self.__csv_file = pandas.read_csv(csv_path)

    def get_csv_file(self):
        '''
        Returns the csv file contents
        '''
        return self.__csv_file

    def get_questions_best_is_max(self):
        '''
        Returns "questions_best_is_max" list contents
        '''
        return self.__questions_best_is_max

    def get_questions_best_is_min(self):
        '''
        Returns "questions_best_is_min" list contents
        '''
        return self.__questions_best_is_min
