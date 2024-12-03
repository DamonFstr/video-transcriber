import os
import argparse
from google.cloud import speech, storage
from pydub import AudioSegment
import io

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<insert creds here>"

def extract_audio(video_file, output_audio_file):
    if os.path.exists(output_audio_file):
        print(f"Audio already extracted: {output_audio_file}")
        return
    audio = AudioSegment.from_file(video_file)
    audio.export(output_audio_file, format="wav")
    print(f"Audio extracted: {output_audio_file}")

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket if it's not already uploaded."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    if blob.exists():
        print(f"File already exists in GCS: {destination_blob_name}")
        return f"gs://{bucket_name}/{destination_blob_name}"

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    return f"gs://{bucket_name}/{destination_blob_name}"

def transcribe_audio_gcs(gcs_uri, output_transcript_file):
    if os.path.exists(output_transcript_file):
        print(f"Transcript already exists: {output_transcript_file}")
        with open(output_transcript_file, "r") as file:
            return file.read()

    client = speech.SpeechClient()
    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        language_code="en-US",
        audio_channel_count=2
    )

    operation = client.long_running_recognize(config=config, audio=audio)
    print("Waiting for operation to complete...")
    response = operation.result(timeout=600)

    transcript = []
    for result in response.results:
        transcript.append(result.alternatives[0].transcript)
    
    transcript_text = " ".join(transcript)
    save_transcript_to_file(transcript_text, output_transcript_file)
    
    return transcript_text

def save_transcript_to_file(transcript, output_file):
    with open(output_file, "w") as file:
        file.write(transcript)
    print(f"Transcript saved to {output_file}")

def process_video(video_file, bucket_name):
    base_name = os.path.splitext(os.path.basename(video_file))[0]
    output_audio_file = f"{base_name}.wav"
    output_transcript_file = f"{base_name}_transcript.txt"
    destination_blob_name = f"{base_name}.wav"

    extract_audio(video_file, output_audio_file)

    gcs_uri = upload_to_gcs(bucket_name, output_audio_file, destination_blob_name)

    transcript = transcribe_audio_gcs(gcs_uri, output_transcript_file)

    print(f"Final Transcript:\n{transcript}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transcribe a video file to text using GCS and asynchronous transcription.")
    parser.add_argument("video_file", help="Path to the video file to transcribe.")
    parser.add_argument("bucket_name", help="Name of the Google Cloud Storage bucket to upload the audio to.")
    args = parser.parse_args()

    process_video(args.video_file, args.bucket_name)
