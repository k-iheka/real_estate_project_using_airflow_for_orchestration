# Import necessary dependencies
import psycopg2
import pandas as pd
import io
import os
from azure.storage.blob import BlobServiceClient, BlobClient
import requests
import json

def run_real_estate_etl():

    # Extraction layer
    url = "https://realty-mole-property-api.p.rapidapi.com/properties"

    querystring = {"limit":"500"}

    headers = {
        "X-RapidAPI-Key": "7fc521eee3msh22f7b3551ecef82p100f89jsn71a07ea1c1d6",
        "X-RapidAPI-Host": "realty-mole-property-api.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)


    data = response.json()

    filename = 'real_estate.json'

    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
        
    #loadin into dataframe
    real_estate_df = pd.read_json('real_estate.json')

    location_dim = real_estate_df[['id','addressLine1', 'city', 'state', 'zipCode', 'formattedAddress', \
                                'county', 'longitude','latitude']].copy().drop_duplicates().reset_index(drop=True)
    location_dim.index.name= 'location_id'
    location_dim= location_dim.reset_index()

    features_dim = pd.json_normalize(real_estate_df['features'])
    features_dim['id'] = real_estate_df['id']
    features_dim['squareFootage'] = real_estate_df['squareFootage']
    features_dim['bedrooms'] = real_estate_df['bedrooms']
    features_dim['bathrooms'] = real_estate_df['bathrooms']
    features_dim.index.name= 'features_id'
    features_dim=features_dim.reset_index()
    features_dim= features_dim[['features_id','id','floorCount', 'architectureType', 'exteriorType', \
                                'foundationType', 'roofType', 'fireplace', 'unitCount', 'garage', \
                                'garageSpaces', 'garageType', 'cooling', 'coolingType', 'heating', \
                                'heatingType', 'roomCount', 'pool', 'fireplaceType', 'poolType', \
                                'squareFootage', 'bedrooms', 'bathrooms']]

    sales_dim= real_estate_df[['id', 'lastSalePrice','lastSaleDate' ]].copy().drop_duplicates().reset_index(drop=True)
    sales_dim.index.name= 'sales_id'
    sales_dim=sales_dim.reset_index()

    tax_assessment_dim= pd.json_normalize(real_estate_df['taxAssessment'])
    tax_assessment_dim.index.name = 'tax_ass_id'
    tax_assessment_dim=tax_assessment_dim.reset_index()
    tax_assessment_dim['id'] = real_estate_df['id']
    tax_assessment_dim=tax_assessment_dim[['tax_ass_id','id', '2019.value', '2019.improvements', '2022.value', \
                                            '2022.improvements', '2023.value', '2023.improvements', '2019.land', \
                                            '2020.value', '2020.land', '2020.improvements', '2022.land', \
                                            '2023.land', '2018.value', '2018.land', '2018.improvements', \
                                            '2021.value', '2021.land', '2021.improvements', '2013.value', \
                                            '2013.land', '2013.improvements', '2014.value', '2014.land', \
                                            '2014.improvements', '2015.value', '2015.land', '2015.improvements', \
                                            '2016.value', '2016.land', '2016.improvements', '2017.value', \
                                            '2017.land', '2017.improvements', '0.date']]

    propertyTax_dim = pd.json_normalize(real_estate_df['propertyTaxes'])
    propertyTax_dim['id'] = real_estate_df['id']
    propertyTax_dim.index.name= 'property_tax_id'
    propertyTax_dim= propertyTax_dim.reset_index()
    propertyTax_dim = propertyTax_dim[['property_tax_id','id','2018.total', '2019.total', '2022.total', '2017.total', '2021.total', \
                                        '2013.total', '2014.total', '2015.total', '2016.total', '2020.total', \
                                        '2023.total']]

    owner_dim= pd.json_normalize(real_estate_df['owner'])
    owner_dim['id']= real_estate_df['id']
    owner_dim.index.name= 'owner_id'
    owner_dim= owner_dim.reset_index()
    owner_dim = owner_dim[['owner_id','id','names', 'mailingAddress.id', 'mailingAddress.addressLine1', \
                            'mailingAddress.city', 'mailingAddress.state', 'mailingAddress.zipCode', \
                            'mailingAddress.formattedAddress', 'mailingAddress.addressLine2']]

    property_fact = real_estate_df.merge(location_dim, on=['id', 'addressLine1', 'city', 'state', 'zipCode', \
                                                        'formattedAddress', 'county', 'longitude', 'latitude'], how='left') \
                                .merge(features_dim, on=['id','squareFootage', 'bedrooms', 'bathrooms'], how='left') \
                                .merge(sales_dim, on=['id', 'lastSalePrice', 'lastSaleDate'], how='left') \
                                .merge(tax_assessment_dim, on='id', how='left') \
                                .merge(propertyTax_dim, on=['id'], how='left') \
                                .merge(owner_dim, on=['id'], how='left') \
                                [['id','location_id', 'property_tax_id', 'features_id', 'sales_id','tax_ass_id','owner_id', \
                                    'assessorID','legalDescription', 'ownerOccupied','yearBuilt', 'zoning', 'lotSize', \
                                    'propertyType','subdivision']]

    #saving to csv
    real_estate_df.to_csv('dataset/rawdata/real_estate.csv', index=False)
    features_dim.to_csv('dataset/cleandata/features.csv', index=False)
    sales_dim.to_csv('dataset/cleandata/sales.csv', index=False)
    tax_assessment_dim.to_csv('dataset/cleandata/tax_assessment.csv', index=False)
    propertyTax_dim.to_csv('dataset/cleandata/propertyTax.csv', index=False)
    owner_dim.to_csv('dataset/cleandata/owner.csv', index=False)
    location_dim.to_csv('dataset/cleandata/location.csv', index=False)
    print('Data correctly written to csv successfully')

    # Setup our connection
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME')

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    def upload_df_to_blob_parquet(df, container_client, blob_name):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(buffer, blob_type="BlockBlob",overwrite=True)
        print(f'{blob_name} uploaded to Blob storage successfully')


    upload_df_to_blob_parquet(real_estate_df, container_client, 'rawdata/real_estate.parquet')
    upload_df_to_blob_parquet(features_dim, container_client, 'cleandata/features.parquet')
    upload_df_to_blob_parquet(sales_dim, container_client, 'cleandata/sales.parquet')
    upload_df_to_blob_parquet(tax_assessment_dim, container_client, 'cleandata/tax_assessment.parquet')
    upload_df_to_blob_parquet(propertyTax_dim, container_client, 'cleandata/propertyTax.parquet')
    upload_df_to_blob_parquet(owner_dim, container_client, 'cleandata/owner.parquet')
    upload_df_to_blob_parquet(location_dim, container_client, 'cleandata/location.parquet')





