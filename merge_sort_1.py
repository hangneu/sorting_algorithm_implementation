def divider(data_array):
	length = len(data_array)
	if(length==0 or length==1): return data_array
	left=divider(data_array[:length//2] if length%2==0 else data_array[:length//2+1])
	right=divider(data_array[length//2:] if length%2==0 else data_array[length//2+1:])
	return conquer(left,right)
def conquer(left, right):
	l, r, result = 0, 0, []
	while(l<len(left) and r < len(right)):
		if left[l] > right[r]:
			result.append(right[r])
			r += 1
		else:
			result.append(left[l])
			l += 1
	return result+left[l:]+right[r:]
