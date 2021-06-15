"""remove stamp

Revision ID: 6b4305d31bb0
Revises: a8c29d33b1c8
Create Date: 2021-06-14 17:26:30.429373

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6b4305d31bb0'
down_revision = 'a8c29d33b1c8'
branch_labels = None
depends_on = None


def upgrade():
    # stamp is dropped. we don't need it since we can define an index on jsonb data (which is equally fast)
    op.drop_column('wampdata', 'stamp')
    op.create_index('ix_wampdata_timestamp', 'wampdata', [sa.text("((data->'timestamp')::decimal)")])
    pass


def downgrade():
    op.drop_index('ix_wampdata_timestamp')
    pass
