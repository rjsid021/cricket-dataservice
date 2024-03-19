arr = [12, 11, 13, 5, 6, 7]

if len(arr)>1:
    mid = len(arr) // 2

    left_arr = arr[:mid]
    right_arr = arr[mid:]

    i = j = k = 0
    while (i<len(left_arr)) & (j<(len(right_arr))):
        arr[k]