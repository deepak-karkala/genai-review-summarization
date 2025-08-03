import pandas as pd
from src.pipelines.ingestion.transform import transform_reviews

def test_transform_reviews():
    # Arrange
    raw_data = {
        'review_id': [1, 2, 3, 4, 5],
        'product_id': ['A', 'A', 'B', 'B', 'C'],
        'user_id': [101, 102, 103, 104, 105],
        'star_rating': [5, 1, 3, 4, 5],
        'review_text': [
            '<p>This is GREAT!</p>',
            'I hate this product. It is stupid.', # Should be filtered by toxicity
            'Too short.', # Should be filtered by length
            'My email is test@example.com', # Should be redacted
            'Un produit fantastique en français.'
        ],
        'created_at': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01'])
    }
    raw_df = pd.DataFrame(raw_data)

    # Act
    transformed_df = transform_reviews(raw_df)

    # Assert
    assert len(transformed_df) == 3 # Should filter 2 rows
    assert transformed_df.iloc[1]['cleaned_text'] == 'my email is [EMAIL]'
    assert transformed_df.iloc[0]['language'] == 'en'
    assert transformed_df.iloc[2]['language'] == 'fr'
    assert 'toxicity_score' in transformed_df.columns