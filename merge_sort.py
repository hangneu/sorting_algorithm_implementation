def conquer(left, right):
	l, r, result = 0, 0, []
	while(l<len(left) and r < len(right)):
		[result,l,r] = [result + [right[r]],l,r+1] if left[l] > right[r] else [result + [left[l]],l+1,r]
	return result+left[l:]+right[r:]
def divider(data_array):
	length = len(data_array)
	if(length==0 or length==1): return data_array
	left=divider(data_array[:length//2] if length%2==0 else data_array[:length//2+1])
	right=divider(data_array[length//2:] if length%2==0 else data_array[length//2+1:])
	return conquer(left,right)