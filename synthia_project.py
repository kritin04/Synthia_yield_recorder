import boto3
import logging
import base64
import json
#from langchain_core.prompts import ChatPromptTemplate
#from langchain_core.runnables import RunnablePassthrough, RunnableParallel
#from langchain_core.output_parsers import StrOutputParser
#from langchain_community.chat_models import BedrockChat
#from langchain_community.retrievers import AmazonKnowledgeBasesRetriever
import wave
from io import BytesIO
from datetime import datetime
import requests
from audio_recorder_streamlit import audio_recorder
import streamlit as st 
import pandas as pd
import re
import os
from word2number import w2n 
import re
from datetime import datetime
import time
bucket_name = "transcribetestkritin"
# Amazon Bedrock - settings
ttp = r'C:\Users\Adithya Sau\Downloads\CowIDs.xlsx'
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIA5FTY7VMV5OJBO7NW'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
s3 = boto3.client(service_name='s3',region_name='ap-south-1')
translate_client = boto3.client(service_name='translate', region_name='ap-south-1', use_ssl=True)
s3_client = boto3.client('s3', region_name='ap-south-1')
def takeCommand():
    # Specify the path to your text file
    
    data=s3.get_object(Bucket="transcribetestkritin", Key=f"speech_to_text/text.json")
    body=data['Body'].read().decode('utf-8')
    data=json.loads(body)
    print(data)
    # Read data from the text file
    
    return data
def update_yield_in_excel(cow_id, new_yield):
    # Read the Excel file
    try:
        df = pd.read_excel(ttp, sheet_name="Sheet1", engine='openpyxl')
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # Ensure yield amount is a float
    try:
        new_yield = float(new_yield)
    except ValueError:
        print("Invalid yield amount.")
        return
    
    # Ensure 'tag_number' and 'yield' columns exist
    if 'tag_number' not in df.columns or 'yield' not in df.columns:
        raise ValueError("The required columns ('tag_number' or 'yield') do not exist in the Excel file.")
    
    # Find the row where the cow ID matches and update the yield
    df.loc[df['tag_number'] == cow_id, 'yield'] = new_yield
    
    # Write the DataFrame back to the Excel file
    try:
        df.to_excel(ttp, sheet_name="Sheet1", index=False, engine='openpyxl')
    except Exception as e:
        print(f"Error writing to Excel file: {e}")
def normalize_text(text):
    
    # Remove all non-alphanumeric characters and convert to lowercase
    return re.sub(r'[^a-zA-Z0-9\s]', ' ', text).lower()

   


def convert_numerical_words(text):
    # Regular expression to find all numerical word sequences
    pattern = re.compile(r'\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|'
                        r'eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|'
                        r'thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|million|billion|trillion)\b(?:[\s-](?:zero|one|two|three|four|five|six|seven|eight|nine|ten|'
                        r'eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|'
                        r'thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|million|billion|trillion))*')

    def replace_num_words(match):
        num_text = match.group(0)
        return str(w2n.word_to_num(num_text))

    return pattern.sub(replace_num_words, text)

def normalize_text(text):
    
    # Remove all non-alphanumeric characters and convert to lowercase
    return re.sub(r'[^a-zA-Z0-9\s]', ' ', text).lower()

def extract_info(text):

    if not text:
        return None, None
    try:
        df = pd.read_excel(ttp, sheet_name='Sheet1')
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None, None
        
        # Print column names to verify
    print("Column names in the Excel file:", df.columns.tolist())

        # Ensure 'name' column exists
    if 'tag_number' not in df.columns:
        raise ValueError("The 'tag_number' column does not exist in the Excel file.")
        
    cow_ids = df["tag_number"].tolist()
    
    cow_ids = list(map(str, cow_ids))
    
    sentence=text[0]['text']
    conv_text=convert_numerical_words(sentence)
    
    
    normalized_text = normalize_text(conv_text)
    
    # Initialize variables to store the found cow ID and yield
    words = conv_text.lower().split()
    # Extract tag number
    tag_index = words.index('number') if 'number' in words else -1
    tag_number = words[tag_index + 1] if tag_index != -1 and tag_index + 1 < len(words) else None
   
    # Extract milk yield
    if 'litres' in words:
        milk_index = words.index('litres')
    elif 'liters' in words:
        milk_index = words.index('liters')
    elif 'litre' in words:
        milk_index = words.index('litre')
    elif 'liter' in words:
        milk_index = words.index('liter')
    else:
        milk_index = -1
    
    milk_yield = words[milk_index - 1] if milk_index > 0 else None
    
    found_cow_id = None
    yield_amount = None
    for cow_id in cow_ids:
        # Preprocess the cow ID
        normalized_cow_id = normalize_text(cow_id)
        #print(normalized_cow_id)
        # Check if the cow ID appears in the text
        if normalized_cow_id in normalized_text:
            found_cow_id = tag_number
            yield_amount=milk_yield
            break
    return found_cow_id, yield_amount


def final_data(tag_number, new_yield):
    # Read the Excel file
    try:
        df = pd.read_excel(ttp, sheet_name="Sheet1", engine='openpyxl')
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    
    
    
    if 'tag_number' not in df.columns:
        raise ValueError("The required column does not exist in the Excel file.")
    
    
    # Find the row where the cow name matches and update the yield
    data = pd.DataFrame()
    data[['farm_name', 'deviceid', 'tag_number']] = df[['farm_name', 'deviceid', 'tag_number']]
    
    text = {
    'tag_number': [tag_number],
    'yield': [new_yield],
    'date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        }
    extracted = pd.DataFrame(text)
    # final_df = pd.merge( data, extracted, right_on=['tag_number'], left_on=['tag_number'])
    # last_df = final_df.to_json()
    data['tag_number'] = data['tag_number'].astype(str)
    extracted['tag_number'] = extracted['tag_number'].astype(str)
    extracted['date']=pd.to_datetime(extracted['date'])
    return extracted, data


    

# Initialize the boto3 client with the credentials
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="ap-south-1"
)
#initializing the polly for text to speech
polly_client = boto3.client(
    service_name="polly",
    region_name="ap-south-1"
)
#intializing the amzon translate
translate = boto3.client(
    service_name="translate",
    region_name="ap-south-1"
)
s3 = boto3.client("s3", region_name="ap-south-1")
transcribe = boto3.client("transcribe", region_name="ap-south-1")
#model_id = "anthropic.claude-3-haiku-20240307-v1:0"
#model_id = "anthropic.claude-3-sonnet-20240229-v1:0"

#defining the text to speech function
def text_to_speech(text, voice_id="Aditi"):
    try:
        response = polly_client.synthesize_speech(
            Text=text,
            OutputFormat='mp3',
            VoiceId=voice_id,
            LanguageCode='hi-IN'  # Ensure the LanguageCode is set to Hindi
        )
        audio_stream = response['AudioStream'].read()
        return base64.b64encode(audio_stream).decode('utf-8')
    except Exception as e:
        st.error(f"Error in text-to-speech conversion: {e}")
        return None
    
#defining the translate function to translate into different languages


def save_audio_to_wav(audio_bytes, filename="confirmation.wav"):
    audio_io = BytesIO(audio_bytes)
    with wave.open(audio_io, 'rb') as wf:
        with wave.open(filename, 'wb') as output_wav:
            output_wav.setnchannels(wf.getnchannels())
            output_wav.setsampwidth(wf.getsampwidth())
            output_wav.setframerate(wf.getframerate())
            output_wav.writeframes(wf.readframes(wf.getnframes()))



def upload_to_s3(filename, bucket, object_name=None):
    if object_name is None:
        object_name = filename
    try:
        s3.upload_file(filename, bucket, object_name)
        return True
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        return False

def transcribe_speech(file_path):
    # Generate a unique job name with timestamp
    job_name = f"transcription_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': file_path},
            MediaFormat='wav',
            LanguageCode='hi-IN'
        )
    except Exception as e:
        logging.error(f"Failed to start transcription job: {e}")
        return ""
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        logging.info("Transcribing...")
    if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
        try:
            response = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            transcript_url = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
            
            # Fetch the transcript JSON content from the URL
            response = requests.get(transcript_url)
            response.raise_for_status()
            transcript_json = response.json()
            
            # Extract the transcript text from the JSON
            transcript_text = transcript_json['results']['transcripts'][0]['transcript']
            return transcript_text
        except Exception as e:
            logging.error(f"Failed to fetch transcript from URL: {e}")
            return ""
    else:
        logging.error("Transcription failed")
        return ""
    
    file_uri=f"s3://{bucket_name}/output.wav"
    text=transcribe_speech(file_uri)

def translate_texti(text, source_language, target_language):
    try:
        response = translate.translate_text(
            Text=text,
            SourceLanguageCode=source_language,
            TargetLanguageCode=target_language
        )
        return response['TranslatedText']
    except Exception as e:
        logging.error(f"Error during translation: {e}")
        return text
    

# Streamlit main app function
def main():
    st.title("Chat Bot")
    st.write("Ask me anything about cow related queries!")
    # Language selection
    languages = {
        "English": "en",
        "Hindi": "hi"
    }
    language_choices = list(languages.keys())
    selected_language = st.selectbox("Select your language:", language_choices)
    selected_language_code = languages[selected_language]
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    chat_container = st.container()
    user_input_container = st.container()
    with chat_container:
        for i, chat in enumerate(st.session_state.chat_history):
            st.write(chat)
            if chat.startswith("Synthia:"):
                if st.button(f"🔊", key=f"play_audio_{i}"):
                    audio_base64 = text_to_speech(chat[8:])
                    if audio_base64:
                        st.audio(base64.b64decode(audio_base64), format='audio/mp3')
    with user_input_container:
        with st.form(key='input_form_unique', clear_on_submit=True):
            user_input = st.text_input(f"You ({selected_language}): ", key="user_input")
            submit_button = st.form_submit_button(label='Send')
        # Adding live speech input button
        audio_bytes = audio_recorder(
            text="Click to record",
            recording_color="#FF0000",
            neutral_color="#FFFFFF"
        )
        if audio_bytes is not None:
            save_audio_to_wav(audio_bytes)
            st.audio(audio_bytes, format='audio/wav')
            st.session_state.chat_history.append(f"You ({selected_language}): [Audio Message]")
            if upload_to_s3("confirmation.wav", bucket_name, "confirmation.wav"):
                file_uri = f"s3://{bucket_name}/confirmation.wav"
                user_input = transcribe_speech(file_uri)
                if user_input:
                    st.session_state.chat_history.append(f"Transcription: {user_input}")
            else:
                st.error("Failed to upload audio to S3.")
        if submit_button and user_input:
            st.session_state.chat_history.append(f"You ({selected_language}): {user_input}")
            try:
                logging.debug(f"User input: {user_input}")
                translated_input = translate_texti(user_input, "hi-IN", "en")
                logging.debug(f"translated text: {translated_input}")
                sam ={
                "text":translated_input
                }
                jd = json.dumps([sam])
                try: 
                    key = f"speech_to_text/text.json"
                    s3.put_object(Body=jd,Bucket=bucket_name,Key=key)
                #aishna's code (extraction)
                #adithya's code (text to speech audio streamlit)
                
                except Exception as e:
                    print(str(e))
                
                command = takeCommand()
                if command:
                    cow_id, yield_amount = extract_info(command)
                    if cow_id and yield_amount:
                        print(f"Tag number: {cow_id}, Yield Amount: {yield_amount} litres")
                        extracted, data=final_data(cow_id, yield_amount)
                        final_df = pd.merge(data, extracted, right_on=['tag_number'], left_on=['tag_number'])
                        last_df = final_df.to_json()
                        print(last_df)
                        file=json.dumps(last_df)
                        key = "Extracted_text/extracted_text.json"
                        s3.put_object(Body=file, Bucket="transcribetestkritin", Key=key)
                    else:
                        print("No cow ID or yield amount found in the input.")
                else:
                    print("Failed to read file")
                #key = f"{bucket_name}/transcribetextkritin/text.txt"
                #s3.put_object(Bucket=bucket_name, body=translated_input, Key=key)
                #object_name="translated.txt"
                
                #st.session_state.chat_history.append(f"Synthia: {translated_response}")
                try:
                    bucket_nam = "transcribetestkritin"
                    key = "Extracted_text/extracted_text.json"
                    response = s3_client.get_object(Bucket=bucket_nam, Key=key)
                    x = json.loads(response['Body'].read().decode('utf-8'))
                    json_dict = json.loads(x)
                    if isinstance(x, str):
                        x = json.loads(x)
                    for key in x:
                        result = translate_client.translate_text(
                            Text=str(x[key]["0"]),
                            SourceLanguageCode="en",
                            TargetLanguageCode="hi"
                        )
                        x[key]["0"] = result.get('TranslatedText')
                        
                    tag_number = x['tag_number']['0']
                    milk_yield = x['yield']['0']
                    farm_name = x['farm_name']['0']
                    timestamp_ms = int(x['date']['0'])

# Convert the timestamp from milliseconds to seconds
                    timestamp_s = timestamp_ms / 1000.0
                    date = datetime.fromtimestamp(timestamp_s)
                    date_time = date.strftime('%Y-%m-%d %H:%M:%S')
                    hindi_string = f"आपके खेत का नाम {farm_name}, इस {date_time} पर , गाय आईडी {tag_number} ने {milk_yield} किलो दूध  दिया 100 | क्या ये सही है?"
                    audio_base64 = text_to_speech(hindi_string, voice_id="Aditi")

# Convert the timestamp from milliseconds to seconds
                   
                    #
                    hindi_string = f"आपके खेत का नाम {farm_name}, इस {date_time} पर , गाय आईडी {tag_number} ने {milk_yield} किलो दूध  दिया| क्या ये सही है?"
                    audio_base64 = text_to_speech(hindi_string, voice_id="Aditi")
                
                    
                    if audio_base64:
                        audio_bytes = base64.b64decode(audio_base64)
                        st.audio(audio_bytes, format='audio/mp3')
                        st.write(hindi_string)
                        time.sleep(10)
                
                except Exception as e:
                    logging.error(f"Error during Polly TTS: {e}")
                    st.error(f"Error during TTS: {e}")
                
            except Exception as e:
                logging.error(f"Error during chain invocation: {e}")
                st.error(f"Error during invocation: {e}")
            


if __name__ == '__main__':
    main()
