import gspread
from google.oauth2.service_account import Credentials


class CloudConnection:
    """
    CloudConnection class is used to connect to google cloud services.
    
    """
    def __init__(self, key_file: str) -> None:
        """
        Initialize the CloudConnection object.
        
        Args:
            key_file (str): path to the key file
        """
        self.credentials = Credentials.from_service_account_file(
            key_file,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        self._client = gspread.authorize(self.credentials)
