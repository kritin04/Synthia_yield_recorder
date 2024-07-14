import boto3
import logging
import base64
import json
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

ttp = r'CowIDs.xlsx'
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIA5FTY7VMV5OJjyfgjvBO7NW'
os.environ['AWS_SECRET_ACCESS_KEY'] = '8vAiZp1Qcdy5457b8byb9t6bvfgc5x3Vm6W3LPU7DutxicvhjgjFur6/WN/bTDev/mXITUs'

s3 = boto3.client(service_name='s3',region_name='ap-south-1')
translate_client = boto3.client(service_name='translate', region_name='ap-south-1', use_ssl=True)
s3_client = boto3.client('s3', region_name='ap-south-1')
polly_client = boto3.client(
    service_name="polly",
    region_name="ap-south-1"
)
transcribe = boto3.client("transcribe", region_name="ap-south-1")

st.markdown(
    '''
    <style>
    iframe[title="audio_recorder_streamlit.audio_recorder"] {
        height: auto;
    }
    </style>
    ''',
    unsafe_allow_html=True
)


def takeCommand():
    data=s3.get_object(Bucket="transcribetestkritin", Key=f"speech_to_text/text.json")
    body=data['Body'].read().decode('utf-8')
    data=json.loads(body)
    print(data)
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
    'date': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    }
    extracted = pd.DataFrame(text)
    # final_df = pd.merge( data, extracted, right_on=['tag_number'], left_on=['tag_number'])
    # last_df = final_df.to_json()
    data['tag_number'] = data['tag_number'].astype(str)
    extracted['tag_number'] = extracted['tag_number'].astype(str)
    final_df = pd.merge(data, extracted, right_on=['tag_number'], left_on=['tag_number'])
    json_output = final_df.to_json(orient='records', date_format='iso')
    #extracted['date']=pd.to_datetime(extracted['date'])
    return json_output

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
    
def save_audio_to_wav(audio_bytes, filename="confirmation.wav"):
    audio_io = BytesIO(audio_bytes)
    with wave.open(audio_io, 'rb') as wf:
        with wave.open(filename, 'wb') as output_wav:
            output_wav.setnchannels(wf.getnchannels())
            output_wav.setsampwidth(wf.getsampwidth())
            output_wav.setframerate(wf.getframerate())
            output_wav.writeframes(wf.readframes(wf.getnframes()))
            
def save_audio_to_wav_conf(audio_bytes, filename="confirmation_response.wav"):
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
    
def translate_text(text, source_language, target_language):
    try:
        response = translate_client.translate_text(
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
    global cow_id, yield_amount
    st.title("Yield Recorder")
    st.write("Tell me what is the yield of your cow!")
    
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

    with user_input_container:  # input to take the audio
        audio_bytes = audio_recorder(
            text="Click to record",
            recording_color="#FF0000",
            neutral_color="#FFFFFF"
        )
        if audio_bytes is not None:  # input tag number, yield
            save_audio_to_wav(audio_bytes)
            st.audio(audio_bytes, format='audio/wav')
            st.session_state.chat_history.append("You (Hindi): [Audio Message]")
            if upload_to_s3("confirmation.wav", bucket_name, "confirmation.wav"):
                file_uri = f"s3://{bucket_name}/confirmation.wav"
                user_input = transcribe_speech(file_uri)
                if user_input:
                    st.session_state.chat_history.append(f"Transcription: {user_input}")
                else:
                    st.error("Failed to transcribe audio.")
            else:
                st.error("Failed to upload audio to S3.")
            
            if user_input:
                try:
                    logging.debug(f"User input: {user_input}")
                    translated_input = translate_text(user_input, "hi-IN", "en")
                    logging.debug(f"Translated text: {translated_input}")
                    sam = {
                        "text": translated_input
                    }
                    jd = json.dumps([sam])
                    try:
                        key = f"speech_to_text/text.json"
                        s3.put_object(Body=jd, Bucket=bucket_name, Key=key)
                    except Exception as e:
                        print(str(e))
                    command = takeCommand()
                    if command:
                        cow_id, yield_amount = extract_info(command)
                        if cow_id and yield_amount:
                            print(f"Tag number: {cow_id}, Yield Amount: {yield_amount} litres")
                            final_json = final_data(cow_id, yield_amount)
                            json_body = json.loads(final_json)
                            json_dict = json_body[0]
                            key = f"Extracted_text/{json_dict['farm_name']}/{json_dict['deviceid']}/extracted.json"
                            s3.put_object(Body=final_json, Bucket=bucket_name, Key=key)
                            
                            # Request confirmation
                            hindi_string = f"आपके खेत का नाम {json_dict['farm_name']} गाय आईडी {json_dict['tag_number']} ने {json_dict['yield']} किलो {json_dict['date']} दूध दिया| क्या ये सही है?"
                            audio_base64 = text_to_speech(hindi_string, voice_id="Aditi")
                            if audio_base64:
                                audio_bytes = base64.b64decode(audio_base64)
                                st.audio(audio_bytes, format='audio/mp3')
                                st.write(hindi_string)
                                time.sleep(10)
##----------------------------------------------------- under process -------------------------------------------------------------#####                            
                            confirm_audio_bytes = audio_recorder(
                                text="Click to confirm",
                                recording_color="#00FF00",  # Green color
                                neutral_color="#FFFFFF"
                            )
                            # confirmation audio processing will be start from this
                            if confirm_audio_bytes is not None:
                                save_audio_to_wav(confirm_audio_bytes, filename="confirmation_response.wav")
                                st.audio(confirm_audio_bytes, format='audio/wav')
                                if upload_to_s3("confirmation_response.wav", bucket_name, "confirmation_response.wav"):
                                    file_uri = f"s3://{bucket_name}/confirmation_response.wav"
                                    confirm_text = transcribe_speech(file_uri)
                                    confirm_text_en = translate_text(confirm_text, "hi-IN", "en")
                                    confirm_list = ["yes","yes.","YES","YES.","yes,","YES,"]
                                    if confirm_text_en.lower() in confirm_list:
                                        st.success("Confirmation received. Data stored successfully.")
                                        try:
                                            key = f"milk_data/{json_dict['farm_name']}/text.json"
                                            json_body = json.dumps(json_dict).encode('utf-8')
                                            s3.put_object(Body=json_body, Bucket=bucket_name, Key=key)
                                            print(f"data pushed to milk data")
                                        except Exception as e:
                                            logging.error(f"Error storing data to S3: {e}")
                                    else:
                                        st.error("Confirmation denied. Please provide the correct data.")
                                        
                                else:
                                    st.error("Failed to upload confirmation audio to S3.")
                        else:
                            print("No cow ID or yield amount found in the input.")
                    else:
                        print("Failed to read file")
                except Exception as e:
                    logging.error(f"Error during taking the input: {e}")
                    st.error(f"Error during input: {e}")
if __name__ == '__main__':
    main()
