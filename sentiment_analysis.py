# Install necessary libraries (if not already installed)
# !pip install transformers datasets torch

import os
import torch
from datasets import load_dataset
from transformers import RobertaTokenizer, RobertaForSequenceClassification, Trainer, TrainingArguments

# Set max_split_size_mb to avoid fragmentation
os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'max_split_size_mb:32'

# Clear CUDA cache before starting
torch.cuda.empty_cache()

# Step 1: Load the Dataset
dataset = load_dataset("glue", "sst2")

# Step 2: Tokenize the Dataset with reduced sequence length
tokenizer = RobertaTokenizer.from_pretrained('roberta-base')

def tokenize_function(examples):
    return tokenizer(examples['sentence'], padding='max_length', truncation=True, max_length=128)  # Reduced max_length

train_dataset = dataset['train'].map(tokenize_function, batched=True)
valid_dataset = dataset['validation'].map(tokenize_function, batched=True)
test_dataset = dataset['test'].map(tokenize_function, batched=True)

# Set the format for PyTorch
train_dataset.set_format('torch', columns=['input_ids', 'attention_mask', 'label'])
valid_dataset.set_format('torch', columns=['input_ids', 'attention_mask', 'label'])
test_dataset.set_format('torch', columns=['input_ids', 'attention_mask', 'label'])

# Step 3: Load the Model
model = RobertaForSequenceClassification.from_pretrained('roberta-base', num_labels=2)

# Step 4: Fine-Tune the Model with Further Optimizations
training_args = TrainingArguments(
    output_dir='./results',                # output directory
    evaluation_strategy='epoch',           # evaluate at the end of each epoch
    per_device_train_batch_size=1,         # Minimum batch size to avoid memory issues
    per_device_eval_batch_size=1,          # Minimum batch size for evaluation
    gradient_accumulation_steps=8,         # Increase accumulation steps to simulate larger batch size
    num_train_epochs=3,                    # number of training epochs
    weight_decay=0.01,                     # strength of weight decay
    logging_dir='./logs',                  # directory for storing logs
    logging_steps=10,
    fp16=True,                             # Enable mixed precision training to reduce memory usage
)

trainer = Trainer(
    model=model,                           # the instantiated 🤗 Transformers model to be trained
    args=training_args,                    # training arguments, defined above
    train_dataset=train_dataset,           # training dataset
    eval_dataset=valid_dataset,            # evaluation dataset
)

# Train the model
trainer.train()

# Step 5: Evaluate the Model
results = trainer.evaluate(test_dataset)
print(f"Test Results: {results}")

# Step 6: Predict Sentiment for New Transcriptions
def predict_sentiment(text):
    inputs = tokenizer(text, return_tensors='pt', padding=True, truncation=True, max_length=128)
    outputs = model(**inputs)
    probs = outputs.logits.softmax(dim=-1)
    sentiment_score = probs[0][1].item() * 100  # Assuming class 1 is positive sentiment
    return sentiment_score

# Example usage
transcription = "This is a fantastic product!"
score = predict_sentiment(transcription)
print(f'Sentiment score: {score}')
