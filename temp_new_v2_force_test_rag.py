from datetime import datetime

import dlt
import psycopg2

import env
from utils.workflow_helper import document_state_resource


def force_state():
    conn = psycopg2.connect(env.DATABASE_URL)
    item_id = "af28f900-429e-11f1-ac02-3babf827b65c"  # One of the test items

    # Move to step 10 (Parent of Summary)
    final_step_id = 10

    pipeline = dlt.pipeline(
        destination="postgres",
        dataset_name="public",
        pipeline_name="force_state_test",
    )

    pipeline.run(
        document_state_resource(
            workflow_id=final_step_id,
            item_ids=[item_id],
            start_time=datetime.now(),
            end_time=datetime.now(),
        )
    )
    print(f"Forced item {item_id} to step {final_step_id}")
    conn.close()


if __name__ == "__main__":
    force_state()
