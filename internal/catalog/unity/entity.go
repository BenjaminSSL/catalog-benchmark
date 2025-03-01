package unity

type Catalog struct {
	Name       string `json:"name"`
	Comment    string `json:"comment"`
	Properties struct {
	} `json:"properties"`
	Owner     interface{} `json:"owner"`
	CreatedAt int64       `json:"created_at"`
	CreatedBy interface{} `json:"created_by"`
	UpdatedAt int64       `json:"updated_at"`
	UpdatedBy interface{} `json:"updated_by"`
	Id        string      `json:"id"`
}

type ListCatalogsResponse struct {
	Catalogs      []Catalog `json:"catalogs"`
	NextPageToken string    `json:"next_page_token"`
}

type CreateCatalogBody struct {
	Name       string      `json:"name"`
	Comment    string      `json:"comment,omitempty"`
	Properties interface{} `json:"properties,omitempty"`
}

type UpdateCatalogBody struct {
	Comment    string      `json:"comment,omitempty"`
	Properties interface{} `json:"properties,omitempty"`
	NewName    string      `json:"new_name,omitempty"`
}
