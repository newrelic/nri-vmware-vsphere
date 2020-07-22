package tag

import (
	"context"
	"github.com/sirupsen/logrus"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vapi/rest"
	_ "github.com/vmware/govmomi/vapi/simulator"
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
)

func Test_CollectTagsByID(t *testing.T) {
	simulator.Run(func(ctx context.Context, vc *vim25.Client) error {
		c := rest.NewClient(vc)
		err := c.Login(ctx, simulator.DefaultLogin)
		assert.NoError(t, err)

		m := tags.NewManager(c)
		categoryName := "my-category"
		categoryID, err := m.CreateCategory(ctx, &tags.Category{Name: categoryName})
		assert.NoError(t, err)

		tagName := "vm-tag"
		tagID, err := m.CreateTag(ctx, &tags.Tag{CategoryID: categoryID, Name: tagName})
		assert.NoError(t, err)

		collector := NewCollector(m, logrus.StandardLogger())
		err = collector.BuildTagCache()
		assert.NoError(t, err)
		assert.Equal(t, categoryName, collector.GetTagByID(tagID).Category)
		assert.Equal(t, tagName, collector.GetTagByID(tagID).Name)

		return nil
	})
}

func Test_GetTags_ReturnsObjectTags(t *testing.T) {
	simulator.Test(func(ctx context.Context, vc *vim25.Client) {
		c := rest.NewClient(vc)
		_ = c.Login(ctx, simulator.DefaultLogin)

		m := tags.NewManager(c)

		categoryName := "my-category"
		categoryID, err := m.CreateCategory(ctx, &tags.Category{
			AssociableTypes: []string{"VirtualMachine"},
			Cardinality:     "SINGLE",
			Name:            categoryName,
		})
		assert.NoError(t, err)
		tagName := "vm-tag"
		tagID, err := m.CreateTag(ctx, &tags.Tag{CategoryID: categoryID, Name: tagName})
		assert.NoError(t, err)

		collector := NewCollector(m, logrus.StandardLogger())
		err = collector.BuildTagCache()
		assert.NoError(t, err)

		vm, err := find.NewFinder(vc).VirtualMachine(ctx, "DC0_H0_VM0")
		assert.NoError(t, err)
		err = m.AttachTag(ctx, tagID, vm.Reference())
		assert.NoError(t, err)

		vms := []mo.Reference{vm.Reference()}
		tagsByCategory, _ := collector.getTags(vms)
		assert.Len(t, tagsByCategory, 1)
		assert.NotEmpty(t, tagsByCategory[vm.Reference()][0])
		assert.Equal(t, tagName, tagsByCategory[vm.Reference()][0].Name)

	})
}

func Test_GetTagsByCategories_ReturnsOrderedTagsPerCategory(t *testing.T) {
	ref := mor{Type: "type", Value: "val"}
	ts := []Tag{
		{
			Name:     "A",
			Category: "cat1",
		},
		{
			Name:     "B",
			Category: "cat1",
		},
		{
			Name:     "B",
			Category: "cat2",
		},
		{
			Name:     "A",
			Category: "cat2",
		},
	}
	tagsByObject := make(map[mor][]Tag)
	tagsByObject[ref] = ts

	// we can use an "fake" manager since we're not using the simulator
	collector := NewCollector(&tags.Manager{}, logrus.StandardLogger())
	collector.cacheTags(tagsByObject)

	tbc := collector.GetTagsByCategories(ref)
	assert.Equal(t, "A|B", tbc["cat1"], "Tags should should be ordered")
	assert.Equal(t, "A|B", tbc["cat2"], "Tags should should be ordered")
}

func Test_ParseTagFilterExpression_CreatesTagFilter(t *testing.T) {

	// we can use an "fake" manager since we're not using the simulator
	collector := NewCollector(&tags.Manager{}, logrus.StandardLogger())

	tests := []struct {
		name string
		args string
		want []Tag
	}{
		{
			name: "InvalidExpression",
			args: "key value",
			want: nil,
		},
		{
			name: "InvalidExpression",
			args: "key:value",
			want: nil,
		},
		{
			name: "SingleTag",
			args: "region=eu",
			want: []Tag{{Category: "region", Name: "eu"}},
		},
		{
			name: "MultipleTags",
			args: "region=eu env=test",
			want: []Tag{{Category: "region", Name: "eu"}, {Category: "env", Name: "test"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// when
			collector.ParseFilterTagExpression(tt.args)

			// then
			assert.Equal(t, len(tt.want), len(collector.filterTags))
			assert.EqualValues(t, tt.want, collector.filterTags)
		})
	}
}

func Test_MatchObjectsTags_ReturnsCorrectValue(t *testing.T) {

	// we can use an "fake" manager since we're not using the simulator
	collector := NewCollector(&tags.Manager{}, logrus.StandardLogger())
	collector.ParseFilterTagExpression("region=eu env=test")

	tests := []struct {
		name string
		args []Tag
		want bool
	}{
		{
			name: "EmptyTagsReturnsFalse",
			args: []Tag{},
			want: false,
		},
		{
			name: "NonExistingCategoryReturnsFalse",
			args: []Tag{{Category: "non-existing", Name: "eu"}},
			want: false,
		},
		{
			name: "NonExistingTagReturnsFalse",
			args: []Tag{{Category: "region", Name: "asia"}},
			want: false,
		},
		{
			name: "ExistingCategoryAndTagReturnsTrue",
			args: []Tag{{Category: "region", Name: "eu"}},
			want: true,
		},
		{
			name: "MultipleExistingCategoryAndTagReturnsTrue",
			args: []Tag{{Category: "region", Name: "eu"}, {Category: "env", Name: "test"}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			actual := collector.matchTags(tt.args)

			// then
			assert.Equal(t, tt.want, actual)
		})
	}
}
