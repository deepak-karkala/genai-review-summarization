import argparse
import logging
import os
import pandas as pd
from transformers import AutoModelForCausalLM, AutoTokenizer, TrainingArguments
from peft import LoraConfig
from trl import SFTTrainer

logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser()
    # SageMaker environments
    parser.add_argument("--model_dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument("--train_data_dir", type=str, default=os.environ.get("SM_CHANNEL_TRAINING"))
    # Hyperparameters
    parser.add_argument("--base_model_id", type=str, default="mistralai/Mistral-7B-Instruct-v0.1")
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--per_device_train_batch_size", type=int, default=4)

    args, _ = parser.parse_known_args()

    # 1. Load data
    train_file = os.path.join(args.train_data_dir, "train.parquet")
    train_dataset = pd.read_parquet(train_file)
    logging.info(f"Loaded {len(train_dataset)} training records.")

    # 2. Load model and tokenizer
    tokenizer = AutoTokenizer.from_pretrained(args.base_model_id)
    tokenizer.pad_token = tokenizer.eos_token
    
    model = AutoModelForCausalLM.from_pretrained(args.base_model_id, device_map="auto")

    # 3. Configure PEFT/LoRA
    peft_config = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
    )

    # 4. Configure Training Arguments
    training_args = TrainingArguments(
        output_dir=os.path.join(args.model_dir, "checkpoints"),
        per_device_train_batch_size=args.per_device_train_batch_size,
        num_train_epochs=args.epochs,
        logging_steps=10,
        save_strategy="epoch",
        report_to="none",
    )

    # 5. Initialize Trainer
    trainer = SFTTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        peft_config=peft_config,
        dataset_text_field="text",
        max_seq_length=1024,
        tokenizer=tokenizer,
    )

    # 6. Start Training
    logging.info("Starting model fine-tuning...")
    trainer.train()
    logging.info("Training complete.")

    # 7. Save the LoRA adapter
    final_adapter_path = os.path.join(args.model_dir, "adapter")
    trainer.save_model(final_adapter_path)
    logging.info(f"LoRA adapter saved to {final_adapter_path}")

if __name__ == "__main__":
    main()