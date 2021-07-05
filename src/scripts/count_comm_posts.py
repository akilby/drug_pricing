from src.utils import connect_to_mongo
from src.schema import CommentPost


if __name__ == '__main__':
	connect_to_mongo()

	print('Counting total comms .....')
	# total_comms = CommentPost.objects(subreddit='opiates').count()

	print('Counting comms w/ parent ids .....')
	n_op_comms = CommentPost.objects(subreddit='opiates', parent_id__exists=True).count()
	print(n_op_comms)

	# out_str = f"total r/opiates comments: {total_comms}\nr/opiates comments w/ parent_ids: {n_op_comms}"
	out_str = f"r/opiates comments w/ parent_ids: {n_op_comms}"


	with open('results/comment_opiates_counts.txt', 'w') as out_file:
		out_file.write(out_str)

