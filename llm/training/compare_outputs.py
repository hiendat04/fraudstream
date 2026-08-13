import argparse
import json

from mlx_lm import generate, load


def load_prompts(path: str, limit: int) -> list[str]:
    prompts = []
    with open(path) as f:
        for line in f:
            example = json.loads(line)
            prompts.append(example["messages"][0]["content"])
            if len(prompts) >= limit:
                break
    return prompts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-model", default="mlx-community/Qwen2.5-3B-Instruct-4bit")
    parser.add_argument("--adapter-path", default="models/adapters/fraud-explainer-v1")
    parser.add_argument("--valid-path", default="data/valid.jsonl")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()

    prompts = load_prompts(args.valid_path, args.limit)

    base_model, base_tokenizer = load(args.base_model)
    tuned_model, tuned_tokenizer = load(args.base_model, adapter_path=args.adapter_path)

    for prompt in prompts:
        print("=" * 80)
        print(f"PROMPT: {prompt}")
        base_output = generate(base_model, base_tokenizer, prompt=prompt, max_tokens=200)
        tuned_output = generate(tuned_model, tuned_tokenizer, prompt=prompt, max_tokens=200)
        print(f"\nBASE:\n{base_output}")
        print(f"\nFINE-TUNED:\n{tuned_output}")


if __name__ == "__main__":
    main()
