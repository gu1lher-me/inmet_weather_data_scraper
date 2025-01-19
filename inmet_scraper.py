import requests
import os
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass
import logging
import zipfile
import io
import polars as pl


@dataclass
class DownloadResult:
    """Class to store the result of a download and extraction attempt"""
    year: int
    success: bool
    file_path: Optional[str] = None
    error_message: Optional[str] = None


class INMETScraper:
    """Class to handle downloading and extracting specific INMET weather data from Rio de Janeiro station A652"""
    
    def __init__(self, output_dir: str = "rio_a652_station_data", start_year: int = 2019):
        """
        Initialize the INMET scraper
        
        Args:
            output_dir (str): Directory where files will be saved
            start_year (int): First year to consider for downloads
        """
        self.base_url = "https://portal.inmet.gov.br/uploads/dadoshistoricos"
        self.output_dir = output_dir
        self.start_year = start_year
        self.current_year = datetime.now().year
        self.station_identifier = "INMET_SE_RJ_A652_RIO DE JANEIRO"
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        os.makedirs(self.output_dir, exist_ok=True)

    def process_year(self, year: int) -> DownloadResult:
        """
        Download and extract Rio de Janeiro weather data for a specific year
        
        Args:
            year (int): Year to process
            
        Returns:
            DownloadResult: Object containing processing attempt results
        """
        output_file = os.path.join(self.output_dir, f"RJ_A652_{year}.parquet")

        if os.path.exists(output_file):
            self.logger.info(f"File for {year} already exists, skipping...")
            return DownloadResult(year, True, output_file)
        
        try:
            url = f"{self.base_url}/{year}.zip"
            self.logger.info(f"Downloading data for {year}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Reads ZIP file in memory instead of saving to disk
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                rio_files = [
                    name for name in zip_ref.namelist() 
                    if self.station_identifier in name
                ]
                
                if not rio_files:
                    return DownloadResult(
                        year, 
                        False, 
                        error_message=f"No Rio de Janeiro A652 station data found for {year}"
                    )
                
                for station_file in rio_files:
                    with zip_ref.open(station_file) as f:
                        df = pl.read_csv(f, separator=';', encoding='latin1', skip_rows=8)
                        df.write_parquet(output_file)
                        self.logger.info(f"Successfully extracted Rio data for {year}")
                        return DownloadResult(year, True, output_file)
                        
        except requests.exceptions.RequestException as e:
            error_msg = f"Error downloading data for {year}: {str(e)}"
            self.logger.error(error_msg)
            return DownloadResult(year, False, error_message=error_msg)
        except (zipfile.BadZipFile, pl.NoDataError) as e:
            error_msg = f"Error processing ZIP file for {year}: {str(e)}"
            self.logger.error(error_msg)
            return DownloadResult(year, False, error_message=error_msg)
        except Exception as e:
            error_msg = f"Unexpected error processing {year}: {str(e)}"
            self.logger.error(error_msg)
            return DownloadResult(year, False, error_message=error_msg)

    def process_all_years(self) -> List[DownloadResult]:
        """
        Process weather data for all years from start_year to current year
        
        Returns:
            List[DownloadResult]: List of processing results for each year
        """
        results = []
        for year in range(self.start_year, self.current_year + 1):
            result = self.process_year(year)
            results.append(result)
        return results

    def get_successful_downloads(self, results: List[DownloadResult]) -> List[str]:
        """Get list of successfully processed file paths"""
        return [result.file_path for result in results if result.success and result.file_path]

    def get_failed_downloads(self, results: List[DownloadResult]) -> List[tuple[int, str]]:
        """Get list of years and error messages that failed to process"""
        return [(result.year, result.error_message) 
                for result in results if not result.success and result.error_message]
    


from inmet_scraper import INMETScraper

def main():
    scraper = INMETScraper(output_dir="rio_a652_station_data", start_year=2019)
    
    results = scraper.process_all_years()
    
    successful = scraper.get_successful_downloads(results)
    failed = scraper.get_failed_downloads(results)
    
    print(f"\nProcessing Summary:")
    print(f"Successfully processed: {len(successful)} files")
    print(f"Failed processing: {len(failed)} files")
    
    if failed:
        print("\nFailed years and reasons:")
        for year, error in failed:
            print(f"{year}: {error}")

if __name__ == "__main__":
    main()