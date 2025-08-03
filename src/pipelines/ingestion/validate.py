import logging
import pandas as pd
import great_expectations as ge

logging.basicConfig(level=logging.INFO)

def validate_cleaned_data(df: pd.DataFrame) -> bool:
    """
    Validates the cleaned data using a Great Expectations suite.
    """
    logging.info("Validating cleaned data...")
    ge_df = ge.from_pandas(df)
    
    # Define expectations
    ge_df.expect_column_to_exist("review_id")
    ge_df.expect_column_values_to_not_be_null("review_id")
    ge_df.expect_column_values_to_be_unique("review_id")
    ge_df.expect_column_values_to_be_in_set("star_rating", [1, 2, 3, 4, 5])
    ge_df.expect_column_values_to_not_be_null("cleaned_text")
    ge_df.expect_column_values_to_be_in_set("language", ["en", "de", "fr", "es", "it", "nl"]) # Example languages
    
    validation_result = ge_df.validate()
    if not validation_result["success"]:
        logging.error("Data validation failed!")
        logging.error(validation_result)
        return False
        
    logging.info("Data validation successful.")
    return True