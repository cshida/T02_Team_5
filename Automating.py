
import kaggle

# Authenticate using the Kaggle API credentials
kaggle.api.authenticate()

# Define the dataset's URL slug from your provided link
dataset_slug = 'obinnaiheanachor/wisabi-bank-dataset'

# Specify the path where you want to download the dataset and whether to unzip the files
path_to_download = 'c:/Users/ethan/OneDrive/Desktop/TrustBankRawData/'  # Change this to your desired path
unzip_files = True

# Download the dataset
kaggle.api.dataset_download_files(dataset_slug, path=path_to_download, unzip=unzip_files)

