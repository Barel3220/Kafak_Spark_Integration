import pandas as pd
import re

file_path = 'campaign_data_2023.csv'
file_path_w_headers = 'campaign_data_2023_with_headers.csv'

column_names = ['creation_time', 'id', 'url', 'title', 'keywords', 'description']
campaign_dataframe = pd.read_csv(file_path, names=column_names, header=0, quotechar='"', delimiter=',', on_bad_lines='skip')

campaign_dataframe.columns = column_names

campaign_dataframe.to_csv(file_path_w_headers, index=False)

print(campaign_dataframe.isnull().sum())

campaign_dataframe.dropna(subset=['title', 'description'], inplace=True)
campaign_dataframe.reset_index(drop=True, inplace=True)

print(campaign_dataframe.isnull().sum())
print(campaign_dataframe.shape)


# Function to check if the string contains only English characters
def is_english(text):
    # Regex pattern for allowed characters (English letters, numbers, and common punctuation)
    pattern = re.compile(r'^[a-zA-Z0-9\s,.;:!?\'"-]*$')
    return bool(pattern.match(text))


# Apply the filter to the 'description' column
df_filtered = campaign_dataframe[campaign_dataframe['description'].apply(is_english)]

# Save the filtered DataFrame to a new CSV file
df_filtered.to_csv('filtered_file.csv', index=False)

print(df_filtered.head())
print(df_filtered.shape)
