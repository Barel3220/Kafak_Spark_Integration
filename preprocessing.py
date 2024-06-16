import pandas as pd
import re

file_path = 'campaign_data_2023.csv'
file_path_w_headers = 'campaign_data_2023_with_headers.csv'

column_names = ['creation_time', 'id', 'url', 'title', 'keywords', 'description']

# Load the CSV file into a DataFrame, specifying column names and handling bad lines
campaign_dataframe = pd.read_csv(file_path, names=column_names, header=0, quotechar='"', delimiter=',', on_bad_lines='skip')

campaign_dataframe.columns = column_names

campaign_dataframe.to_csv(file_path_w_headers, index=False)

print(campaign_dataframe.isnull().sum())

# Drop rows with missing 'title' or 'description' and reset the index
campaign_dataframe.dropna(subset=['title', 'description'], inplace=True)
campaign_dataframe.reset_index(drop=True, inplace=True)

print(campaign_dataframe.isnull().sum())
print(campaign_dataframe.shape)


def is_english(text):
    """Checks if a string contains only English characters.

    Args:
        text (str): The input text to check.

    Returns:
        bool: True if the text contains only English characters, False otherwise.
    """
    # Regex pattern for allowed characters (English letters, numbers, and common punctuation)
    pattern = re.compile(r'^[a-zA-Z0-9\s,.;:!?\'"-]*$')
    return bool(pattern.match(text))


# Apply the filter to the 'description' column to keep only English descriptions
df_filtered = campaign_dataframe[campaign_dataframe['description'].apply(is_english)]

# Save the filtered DataFrame to a new CSV file
df_filtered.to_csv('filtered_file.csv', index=False)

# Print the first few rows and the shape of the filtered DataFrame
print(df_filtered.head())
print(df_filtered.shape)
