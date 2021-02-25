package utils

import (
	"testing"
)

func TestIsValidScope(t *testing.T) {
	type args struct {
		scope string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Invalid scope - it should return false",
			args: args{
				scope: "app1.app2.",
			},
			want: false,
		},
		{
			name: "Invalid scope - it should return false",
			args: args{
				scope: ".app1.app2",
			},
			want: false,
		},
		{
			name: "Invalid scope - it should return false",
			args: args{
				scope: "..app1.app2",
			},
			want: false,
		},
		{
			name: "Invalid scope - it should return false",
			args: args{
				scope: "app1..app2",
			},
			want: false,
		},
		{
			name: "Valid scope - empty scope",
			args: args{
				scope: "",
			},
			want: true,
		},
		{
			name: "Valid scope",
			args: args{
				scope: "app1.app2",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidScope(tt.args.scope); got != tt.want {
				t.Errorf("IsValidScope() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveLastPartInScope(t *testing.T) {
	type args struct {
		scope string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		wantErr bool
	}{
		{
			name: "Invalid scope - should return an error",
			args: args{
				scope: "..app1.app2",
			},
			wantErr: true,
		},
		{
			name: "Valid scope - should return the newScope and the last element",
			args: args{
				scope: "app1.app2.app3",
			},
			want:    "app1.app2",
			want1:   "app3",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := RemoveLastPartInScope(tt.args.scope)
			if (err != nil) != tt.wantErr {
				t.Errorf("RemoveLastPartInScope() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RemoveLastPartInScope() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("RemoveLastPartInScope() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestJoinScopes(t *testing.T) {
	type args struct {
		s1 string
		s2 string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Valid scopes - it should not return an error",
			args: args{
				s1: "app1.app2",
				s2: "app3",
			},
			want:    "app1.app2.app3",
			wantErr: false,
		},
		{
			name: "Valid scopes - scope one as root",
			args: args{
				s1: "",
				s2: "app3",
			},
			want:    "app3",
			wantErr: false,
		},
		{
			name: "Invalid scope in args",
			args: args{
				s1: "app1..app2",
				s2: "app3",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JoinScopes(tt.args.s1, tt.args.s2)
			if (err != nil) != tt.wantErr {
				t.Errorf("JoinScopes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("JoinScopes() = %v, want %v", got, tt.want)
			}
		})
	}
}
