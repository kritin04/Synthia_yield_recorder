# Synthia_Areete
Yield Recorder
Yield Recorder is a Streamlit application designed to record and manage cow milk yield data using speech input in Hindi. It uses AWS services such as S3, Translate, Polly, and Transcribe to handle audio recording, transcription, translation, and text-to-speech functionalities.

# Features

- **Audio Recording**: Record audio directly from the app.
- **Transcription**: Convert recorded audio to text using AWS Transcribe.
- **Translation**: Translate the transcribed text from Hindi to English using AWS Translate.
- **Information Extraction**: Extract cow ID and yield information from the translated text.
- **Excel Integration**: Update the yield information in an Excel file.
- **Text-to-Speech**: Convert text responses to audio using AWS Polly.
- **S3 Integration**: Upload and download files to/from AWS S3.

## Prerequisites

- Python 3.7+
- AWS account with access to S3, Transcribe, Translate, and Polly services
- Streamlit

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
## Set up your AWS credentials
- export AWS_ACCESS_KEY_ID='your-access-key-id'
 -export AWS_SECRET_ACCESS_KEY='your-secret-access-key'

## How to use the app
Click "Click to record" to start recording your audio input in Hindi.

The app will transcribe, translate, and extract the cow ID and yield from your input.

The extracted data will be updated in the Excel file stored in S3 and converted to a JSON format for further use.

### File Structure
- `app.py`: The main Streamlit app.
- `requirements.txt`: List of required Python packages.
- `CowIDs.xlsx`: Initial Excel file containing cow IDs and yield data (to be placed in the same directory).

### Functions

- `takeCommand()`: Retrieve transcription data from S3.
- `update_yield_in_excel(cow_id, new_yield)`: Update the yield information in the Excel file.
- `normalize_text(text)`: Normalize the text by removing non-alphanumeric characters.
- `convert_numerical_words(text)`: Convert numerical words to digits.
- `extract_info(text)`: Extract cow ID and yield information from the text.
- `final_data(tag_number, new_yield)`: Generate final JSON data.
- `text_to_speech(text, voice_id="Aditi")`: Convert text to speech using AWS Polly.
- `save_audio_to_wav(audio_bytes, filename="confirmation.wav")`: Save audio bytes to a WAV file.
- `upload_to_s3(filename, bucket, object_name=None)`: Upload a file to S3.
- `transcribe_speech(file_path)`: Transcribe speech from an audio file using AWS Transcribe.
- `translate_text(text, source_language, target_language)`: Translate text using AWS Translate.
- `main()`: Main function to run the Streamlit app.


## License
   This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgments
- Streamlit
- AWS
- Pandas
- word2number

