import argparse
from jil_parser import JILParser
from airflow_dag_generator import AirflowDAGGenerator

def divide(a, b):
    return a / 0
def main():
    import os
    parser = argparse.ArgumentParser(description='Convert Autosys JIL to Airflow DAG')
    parser.add_argument('input_path', help='Input JIL file or directory path')
    parser.add_argument('output_path', help='Output Python DAG file or directory path')
    parser.add_argument('--dag-id', default='autosys_converted_dag', help='DAG ID (used for single file mode)')
    parser.add_argument('--schedule', help='Schedule interval (e.g., "0 2 * * *")')
    args = parser.parse_args()

    if os.path.isdir(args.input_path):
        # Directory mode: process all .jil files in input_path
        input_dir = args.input_path
        output_dir = args.output_path
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        jil_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.jil')]
        if not jil_files:
            print(f"No JIL files found in directory: {input_dir}")
            return
        for jil_file in jil_files:
            input_file = os.path.join(input_dir, jil_file)
            base_name = os.path.splitext(jil_file)[0]
            output_file = os.path.join(output_dir, f"{base_name}_dag.py")
            jil_parser = JILParser()  # New instance per file
            jobs = jil_parser.parse_file(input_file)
            if not jobs:
                print(f"No jobs found in {jil_file}")
                continue
            print(f"Parsed {len(jobs)} jobs from {jil_file}")
            dag_id = base_name
            generator = AirflowDAGGenerator(jobs)
            dag_code = generator.generate_dag(dag_id, args.schedule)
            with open(output_file, 'w') as f:
                f.write(dag_code)
            print(f"Generated Airflow DAG: {output_file}")
            print("\nConversion Summary:")
            print(f"- Total jobs: {len(jobs)}")
            print(f"- Box jobs: {len([j for j in jobs.values() if j.is_box_job()])}")
            print(f"- File watchers: {len([j for j in jobs.values() if j.is_file_watcher()])}")
            print(f"- Command jobs: {len([j for j in jobs.values() if j.has_command()])}")
            print(f"- Jobs with conditions: {len([j for j in jobs.values() if j.condition])}")
            if not args.schedule:
                detected_schedule = generator._determine_schedule_interval()
                if detected_schedule != "None":
                    print(f"- Detected schedule: {detected_schedule}")
                else:
                    print("- No schedule detected from jobs")
            print()
    else:
        # Single file mode
        jil_parser = JILParser()  # New instance for single file
        jobs = jil_parser.parse_file(args.input_path)
        if not jobs:
            print("No jobs found in JIL file")
            return
        print(f"Parsed {len(jobs)} jobs from JIL file")
        generator = AirflowDAGGenerator(jobs)
        dag_code = generator.generate_dag(args.dag_id, args.schedule)
        with open(args.output_path, 'w') as f:
            f.write(dag_code)
        print(f"Generated Airflow DAG: {args.output_path}")
        print("\nConversion Summary:")
        print(f"- Total jobs: {len(jobs)}")
        print(f"- Box jobs: {len([j for j in jobs.values() if j.is_box_job()])}")
        print(f"- File watchers: {len([j for j in jobs.values() if j.is_file_watcher()])}")
        print(f"- Command jobs: {len([j for j in jobs.values() if j.has_command()])}")
        print(f"- Jobs with conditions: {len([j for j in jobs.values() if j.condition])}")
        if not args.schedule:
            detected_schedule = generator._determine_schedule_interval()
            if detected_schedule != "None":
                print(f"- Detected schedule: {detected_schedule}")
            else:
                print("- No schedule detected from jobs")

if __name__ == "__main__":
    main()