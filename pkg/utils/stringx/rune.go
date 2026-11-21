package stringx

import runewidth "github.com/mattn/go-runewidth"

func init() {
	runewidth.DefaultCondition.EastAsianWidth = true
	runewidth.DefaultCondition.CreateLUT()
}
