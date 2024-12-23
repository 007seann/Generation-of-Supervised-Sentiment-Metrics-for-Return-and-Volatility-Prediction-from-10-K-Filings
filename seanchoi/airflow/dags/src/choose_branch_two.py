
def choose_branch_two(**kwargs):
    ti = kwargs['ti']
    accuracies = ti.xcom_pull(task_ids=[
        'new_data_visualisation',
        'prediction_visualisation',
    ])
    best_accuracy = max(accuracies)
    if best_accuracy > 0:
        return 'good_accuracy'
    return 'bad_accuracy'