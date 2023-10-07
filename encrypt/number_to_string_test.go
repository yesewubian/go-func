package encrypt

import "testing"

func TestNumberToString(t *testing.T) {
	type args struct {
		num    int
		numLen int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test12",
			args: args{
				num:    129,
				numLen: 12,
			},
			want: "llllllllllll",
		},
		{
			name: "test24",
			args: args{
				num:    143429,
				numLen: 24,
			},
			want: "llllllllllllllllllllllll",
		},
		{
			name: "test56",
			args: args{
				num:    143429,
				numLen: 42,
			},
			want: "llllllllllllllllllllllllllllllllllllllllll",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := NumberToString(tt.args.num, tt.args.numLen); len(got) != len(tt.want) || err != nil {
				t.Errorf("NumberToString() = %v, want %v", got, tt.want)
			}
		})
	}
}
