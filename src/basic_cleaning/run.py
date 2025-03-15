#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import os
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download artifact
    logger.info("Downloading Artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Removing all the outliers from the price data
    logger.info("Removing Outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # The step converts the last_review column to a datetime format
    logger.info("Converting last_review to datetime format")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save the cleaned data as an artifact
    logger.info("Saving the cleaned data as an artifact")
    filename_clean_data = "clean_data.csv"
    df.to_csv(filename_clean_data, index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename_clean_data)

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(filename_clean_data)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact name",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum value of price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type= float,
        help="Maximum value of price",
        required=True
    )


    args = parser.parse_args()

    go(args)
