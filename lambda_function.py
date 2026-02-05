from src.iris_ai_parser import parse_email

def lambda_handler(event, context):
    return parse_email(event)