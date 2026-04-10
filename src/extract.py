import pandas as pd

def extract_candidates(path):
    df_candidates = pd.read_csv(path, sep=";")

    df_candidates.columns = [
        "first_name",
        "last_name",
        "email",
        "application_date",
        "country",
        "yoe",
        "seniority",
        "technology",
        "code_challenge_score",
        "technical_interview_score"
    ]

    return df_candidates