def partition(data_array, start, end):
	x = data_array[end-1]
	i = start - 1
	for j in range(start, end-1):
		if data_array[j] <= x:
			i += 1
			data_array[i],data_array[j] = data_array[j],data_array[i]
	data_array[end-1],data_array[i+1] = data_array[i+1], data_array[end-1]
	return i + 1
def quick_sort(data_array, start, end):
	if start < end:
		mid = partition(data_array, start, end)
		quick_sort(data_array, start, mid)
		quick_sort(data_array, mid + 1, end)
A = [1,9,3,6,7,3,80,8,89,43,56,244]
quick_sort(A,1,len(A))
print A