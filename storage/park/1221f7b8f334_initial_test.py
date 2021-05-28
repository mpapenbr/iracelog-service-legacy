"""initial test

Revision ID: 1221f7b8f334
Revises: 
Create Date: 2021-05-18 18:32:18.172925

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1221f7b8f334'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'event',
        sa.Column('id', sa.Integer, primary_key=True,autoincrement=True),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column('data', postgresql.JSONB)
    )


def downgrade():
    op.drop_table('event')
