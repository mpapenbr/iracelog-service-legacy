"""json_merge

Revision ID: 583099217f53
Revises: 6b4305d31bb0
Create Date: 2021-06-14 17:56:10.151676

"""
from alembic import op
import sqlalchemy as sa
from alembic_utils.pg_function import PGFunction

# revision identifiers, used by Alembic.
revision = '583099217f53'
down_revision = '6b4305d31bb0'
branch_labels = None
depends_on = None



def upgrade():
    jsonb_merge = PGFunction(
        schema="public",
        signature="mgm_jsonb_merge(p_val_1 jsonb, p_val_2 jsonb)",
        definition="""
RETURNS jsonb
AS
$body$
/**********************************************************************
 Deep merges two JSON values
 ===========================
 
 For scalar values, arrays or values of different types, the value from the second argument overwrites 
 the value from the first argument (for the same key).

 For "JSON objects, the function recursively merges the values for each key.

 If the value for a key in the second argument is null, the key and it's value
 will be removed completely.
 
 Examples
 ========

 Simple key/value
 ----------------
 mgm_jsonb_merge('{"answer": 1}', 
                 '{"answer": 42}')
 
 returns: {"answer": 42}

 Non-matching types
 ------------------
    
 mgm_jsonb_merge('{"answer": 42}', 
                 '{"answer": {"value": 42}}')
 
 returns: {"answer": {"value": 42}}
 
 Nested types and arrays
 -----------------------
 
 mgm_jsonb_merge('{"answer": 1, "foo": {"one": "x", "two": "y"}, "names": ["one","two"]}', 
                 '{"answer": 42, "foo": {"three": "z"}, "names": ["three"]}')
 
 returns: {"answer": 42, "foo": {"one": "x", "two": "y", "three": "z"}, "names": ["three"]}


 Removing keys
 -------------

 mgm_jsonb_merge('{"answer": 1, "foo": {"one": "x", "two": "y"}}', 
                 '{"foo": {"two": null}}')

 returns: {"answer": 1, "foo": {"one": "x"}}
*********************************************************************/   

  -- if either value is NULL, return the other
  -- don't do any recursive processing
  select coalesce(p_val_1, p_val_2)
  where p_val_1 is null or p_val_2 is null
  
  union all
  
  select 
    -- strip_nulls makes sure we can delete keys from the JSON 
    -- by setting them to null
    jsonb_strip_nulls(
      jsonb_object_agg(
        -- as this is a full outer join, one of the keys could be null
        coalesce(ka, kb), 
          -- process the value for the current key
          -- if either value is null, just use the not null one
          case 
            when va is null then vb
            when vb is null then va
            
            -- both values are objects, merge them recursively
            when jsonb_typeof(va) = 'object' and jsonb_typeof(vb) = 'object' 
              then mgm_jsonb_merge(va, vb) 
              
            -- the two values are not of the same type or are arrays, so return the second one
            -- this overwrites values from the first argument with values from the second
            else vb
          end
      ) -- end of jsonb_object_agg()
    )  -- end of jsonb_strip_nulls()
  from jsonb_each(p_val_1) e1(ka, va) 
    full join jsonb_each(p_val_2) e2(kb, vb) on ka = kb 
  where p_val_1 is not null and p_val_2 is not null;
$body$
  LANGUAGE SQL
  IMMUTABLE
  COST 100
  PARALLEL SAFE;
"""
    )
    op.create_entity(jsonb_merge)
    pass


def downgrade():
    jsonb_merge = PGFunction(
        schema="public",
        signature="mgm_jsonb_merge(p_val_1 jsonb, p_val_2 jsonb)",
        definition="# Not used"
    )
    op.drop_entity(jsonb_merge)
    pass
