import pandas as pd
from fuzzywuzzy import fuzz


facebook_df = pd.read_csv('facebook_dataset.csv', encoding='utf-8', sep=',', quotechar='"', escapechar='\\')
google_df = pd.read_csv('google_dataset.csv', encoding='utf-8', sep=',', quotechar='"', escapechar='\\')
website_df = pd.read_csv('website_dataset.csv', encoding='utf-8', sep=';')


website_df.rename(columns={
    'root_domain': 'domain',
    'main_city': 'city',
    'main_country': 'country_name',
    'main_region': 'region_name',
    'site_name': 'name',
    's_category': 'category'
}, inplace=True)


for df in [facebook_df, google_df, website_df]:
    df['phone'] = df['phone'].astype(str)


merged_df = pd.merge(facebook_df, google_df, on=[
    'domain', 'address', 'city', 'country_code', 'country_name', 'name',
    'phone', 'phone_country_code', 'region_code', 'region_name', 'zip_code'
], how='outer')

merged_df = pd.merge(merged_df, website_df, on=[
    'domain', 'city', 'country_name', 'region_name', 'name', 'phone'
], how='outer')


threshold = int(merged_df.shape[1] * 0.4)
cleaned_df = merged_df.dropna(thresh=threshold)


cleaned_df = cleaned_df[cleaned_df['phone'].str.len().between(10, 15)]


columns_to_clean = ['country_code', 'country_name', 'region_code', 'region_name']
for column in columns_to_clean:
    cleaned_df[column] = cleaned_df[column].str.replace(r'\d+', ' ', regex=True).str.strip()


rows_to_keep = []
for i in range(len(cleaned_df) - 1):
    row1, row2 = cleaned_df.iloc[i], cleaned_df.iloc[i + 1]


    name1 = str(row1['name'] or '')
    name2 = str(row2['name'] or '')


    similarity = fuzz.ratio(name1, name2)


    if similarity >= 60:
        non_null_count1 = row1.notna().sum()
        non_null_count2 = row2.notna().sum()
        rows_to_keep.append(row1 if non_null_count1 >= non_null_count2 else row2)


if len(cleaned_df) > 0:
    rows_to_keep.append(cleaned_df.iloc[-1])


final_df = pd.DataFrame(rows_to_keep)


final_df = final_df.drop_duplicates(subset='phone', keep='first')


final_df = final_df[
    ['category_x', 'address', 'city', 'country_code', 'country_name', 'region_code', 'region_name', 'phone', 'name']]


final_df.to_csv('Combined_database.csv', encoding='utf-8', index=False)


